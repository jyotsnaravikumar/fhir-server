// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Core;
using Microsoft.Health.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Core.Configs;
using Microsoft.Health.Fhir.Core.Exceptions;
using Microsoft.Health.Fhir.Core.Features.Operations.Reindex.Models;
using Microsoft.Health.Fhir.Core.Messages.Reindex;
using Polly;
using Polly.Retry;

namespace Microsoft.Health.Fhir.Core.Features.Operations.Reindex
{
    /// <summary>
    /// The worker responsible for running the reindex job tasks.
    /// </summary>
    public class ReindexJobWorker : IRequestHandler<CancelReindexRequest, CancelReindexResponse>
    {
        private readonly Func<IScoped<IFhirOperationDataStore>> _fhirOperationDataStoreFactory;
        private readonly ReindexJobConfiguration _reindexJobConfiguration;
        private readonly Func<IReindexJobTask> _reindexJobTaskFactory;
        private readonly AsyncRetryPolicy _retryPolicy;
        private readonly ILogger _logger;

        private const int DefaultRetryCount = 3;
        private static readonly Func<int, TimeSpan> DefaultSleepDurationProvider = new Func<int, TimeSpan>(retryCount => TimeSpan.FromSeconds(Math.Pow(2, retryCount)));

        private readonly ConcurrentDictionary<string, (Task, CancellationTokenSource)> _runningTasks;

        public ReindexJobWorker(
            Func<IScoped<IFhirOperationDataStore>> fhirOperationDataStoreFactory,
            IOptions<ReindexJobConfiguration> reindexJobConfiguration,
            Func<IReindexJobTask> reindexJobTaskFactory,
            ILogger<ReindexJobWorker> logger)
        {
            EnsureArg.IsNotNull(fhirOperationDataStoreFactory, nameof(fhirOperationDataStoreFactory));
            EnsureArg.IsNotNull(reindexJobConfiguration?.Value, nameof(reindexJobConfiguration));
            EnsureArg.IsNotNull(reindexJobTaskFactory, nameof(reindexJobTaskFactory));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _fhirOperationDataStoreFactory = fhirOperationDataStoreFactory;
            _reindexJobConfiguration = reindexJobConfiguration.Value;
            _reindexJobTaskFactory = reindexJobTaskFactory;
            _logger = logger;

            _retryPolicy = Policy.Handle<JobConflictException>()
                .WaitAndRetryAsync(DefaultRetryCount, DefaultSleepDurationProvider);

            _runningTasks = new ConcurrentDictionary<string, (Task, CancellationTokenSource)>();
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Remove all completed tasks.
                    var completedTasks = _runningTasks.Where(kvp => kvp.Value.Item1.IsCompleted);
                    foreach (var task in completedTasks)
                    {
                        _runningTasks.TryRemove(task.Key, out var jobPair);
                    }

                    // Get list of available jobs.
                    if (_runningTasks.Count < _reindexJobConfiguration.MaximumNumberOfConcurrentJobsAllowed)
                    {
                        using (IScoped<IFhirOperationDataStore> store = _fhirOperationDataStoreFactory())
                        {
                            _logger.LogTrace("Querying datastore for reindex jobs.");

                            IReadOnlyCollection<ReindexJobWrapper> jobs = await store.Value.AcquireReindexJobsAsync(
                                _reindexJobConfiguration.MaximumNumberOfConcurrentJobsAllowed,
                                _reindexJobConfiguration.JobHeartbeatTimeoutThreshold,
                                cancellationToken);

                            foreach (ReindexJobWrapper job in jobs)
                            {
                                _logger.LogTrace("Picked up reindex job: {jobId}.", job.JobRecord.Id);
                                var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                                _runningTasks.TryAdd(
                                    job.JobRecord.Id,
                                    (_reindexJobTaskFactory().ExecuteAsync(job.JobRecord, job.ETag, source.Token), source));
                            }
                        }
                    }

                    await Task.Delay(_reindexJobConfiguration.JobPollingFrequency, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // End the execution of the task
                }
                catch (Exception ex)
                {
                    // The job failed.
                    _logger.LogError(ex, "Unhandled exception in the worker.");
                    await Task.Delay(_reindexJobConfiguration.JobPollingFrequency, cancellationToken);
                }
            }
        }

        public async Task<CancelReindexResponse> Handle(CancelReindexRequest request, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(request, nameof(request));

            return await _retryPolicy.ExecuteAsync(async () =>
            {
                using (IScoped<IFhirOperationDataStore> store = _fhirOperationDataStoreFactory())
                {
                    ReindexJobWrapper outcome = await store.Value.GetReindexJobByIdAsync(request.JobId, cancellationToken);

                    // If the job is already completed for any reason, return conflict status.
                    if (outcome.JobRecord.Status.IsFinished())
                    {
                        throw new RequestNotValidException(
                            string.Format(Resources.ReindexJobInCompletedState, outcome.JobRecord.Id, outcome.JobRecord.Status));
                    }

                    // Try to cancel the job.
                    outcome.JobRecord.Status = OperationStatus.Canceled;
                    outcome.JobRecord.CanceledTime = Clock.UtcNow;

                    await store.Value.UpdateReindexJobAsync(outcome.JobRecord, outcome.ETag, cancellationToken);

                    if (_runningTasks.TryRemove(request.JobId, out var jobPair))
                    {
                        jobPair.Item2.Cancel();
                    }

                    return new CancelReindexResponse(HttpStatusCode.Accepted, outcome);
                }
            });
        }
    }
}
