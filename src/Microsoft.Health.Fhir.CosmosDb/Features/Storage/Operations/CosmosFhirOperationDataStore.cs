﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Core.Extensions;
using Microsoft.Health.Fhir.Core.Features.Operations;
using Microsoft.Health.Fhir.Core.Features.Operations.Export.Models;
using Microsoft.Health.Fhir.Core.Features.Operations.Reindex.Models;
using Microsoft.Health.Fhir.Core.Features.Persistence;
using Microsoft.Health.Fhir.CosmosDb.Configs;
using Microsoft.Health.Fhir.CosmosDb.Features.Storage.Operations.Export;
using Microsoft.Health.Fhir.CosmosDb.Features.Storage.Operations.Reindex;
using Microsoft.Health.Fhir.CosmosDb.Features.Storage.StoredProcedures.AcquireExportJobs;
using Microsoft.Health.Fhir.CosmosDb.Features.Storage.StoredProcedures.AcquireReindexJobs;

namespace Microsoft.Health.Fhir.CosmosDb.Features.Storage.Operations
{
    public sealed class CosmosFhirOperationDataStore : IFhirOperationDataStore
    {
        private const string HashParameterName = "@hash";

        private static readonly string GetJobByHashQuery =
            $"SELECT TOP 1 * FROM ROOT r WHERE r.{JobRecordProperties.JobRecord}.{JobRecordProperties.Hash} = {HashParameterName} AND r.{JobRecordProperties.JobRecord}.{JobRecordProperties.Status} IN ('{OperationStatus.Queued}', '{OperationStatus.Running}') ORDER BY r.{KnownDocumentProperties.Timestamp} ASC";

        private static readonly string CheckActiveJobsByStatusQuery =
            $"SELECT TOP 1 * FROM ROOT r WHERE r.{JobRecordProperties.JobRecord}.{JobRecordProperties.Status} IN ('{OperationStatus.Queued}', '{OperationStatus.Running}', '{OperationStatus.Paused}')";

        private readonly IScoped<Container> _containerScope;
        private readonly RetryExceptionPolicyFactory _retryExceptionPolicyFactory;
        private readonly ICosmosQueryFactory _queryFactory;
        private readonly ILogger _logger;

        private static readonly AcquireExportJobs _acquireExportJobs = new AcquireExportJobs();
        private static readonly AcquireReindexJobs _acquireReindexJobs = new AcquireReindexJobs();

        /// <summary>
        /// Initializes a new instance of the <see cref="CosmosFhirOperationDataStore"/> class.
        /// </summary>
        /// <param name="containerScope">The factory for <see cref="Container"/>.</param>
        /// <param name="cosmosDataStoreConfiguration">The data store configuration.</param>
        /// <param name="namedCosmosCollectionConfigurationAccessor">The IOptions accessor to get a named version.</param>
        /// <param name="retryExceptionPolicyFactory">The retry exception policy factory.</param>
        /// <param name="queryFactory">The Query Factory</param>
        /// <param name="logger">The logger.</param>
        public CosmosFhirOperationDataStore(
            IScoped<Container> containerScope,
            CosmosDataStoreConfiguration cosmosDataStoreConfiguration,
            IOptionsMonitor<CosmosCollectionConfiguration> namedCosmosCollectionConfigurationAccessor,
            RetryExceptionPolicyFactory retryExceptionPolicyFactory,
            ICosmosQueryFactory queryFactory,
            ILogger<CosmosFhirOperationDataStore> logger)
        {
            EnsureArg.IsNotNull(containerScope, nameof(containerScope));
            EnsureArg.IsNotNull(cosmosDataStoreConfiguration, nameof(cosmosDataStoreConfiguration));
            EnsureArg.IsNotNull(namedCosmosCollectionConfigurationAccessor, nameof(namedCosmosCollectionConfigurationAccessor));
            EnsureArg.IsNotNull(retryExceptionPolicyFactory, nameof(retryExceptionPolicyFactory));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _containerScope = containerScope;
            _retryExceptionPolicyFactory = retryExceptionPolicyFactory;
            _queryFactory = queryFactory;
            _logger = logger;

            CosmosCollectionConfiguration collectionConfiguration = namedCosmosCollectionConfigurationAccessor.Get(Constants.CollectionConfigurationName);

            DatabaseId = cosmosDataStoreConfiguration.DatabaseId;
            CollectionId = collectionConfiguration.CollectionId;
        }

        private string DatabaseId { get; }

        private string CollectionId { get; }

        public async Task<ExportJobOutcome> CreateExportJobAsync(ExportJobRecord jobRecord, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(jobRecord, nameof(jobRecord));

            var cosmosExportJob = new CosmosExportJobRecordWrapper(jobRecord);

            try
            {
                var result = await _containerScope.Value.CreateItemAsync(
                    cosmosExportJob,
                    new PartitionKey(CosmosDbExportConstants.ExportJobPartitionKey),
                    cancellationToken: cancellationToken);

                return new ExportJobOutcome(jobRecord, WeakETag.FromVersionId(result.Resource.ETag));
            }
            catch (CosmosException dce)
            {
                if (dce.IsRequestRateExceeded())
                {
                    throw;
                }

                _logger.LogError(dce, "Failed to create an export job.");
                throw;
            }
        }

        public async Task<ExportJobOutcome> GetExportJobByIdAsync(string id, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNullOrWhiteSpace(id, nameof(id));

            try
            {
                ItemResponse<CosmosExportJobRecordWrapper> cosmosExportJobRecord = await _containerScope.Value.ReadItemAsync<CosmosExportJobRecordWrapper>(
                    id,
                    new PartitionKey(CosmosDbExportConstants.ExportJobPartitionKey),
                    cancellationToken: cancellationToken);

                var outcome = new ExportJobOutcome(cosmosExportJobRecord.Resource.JobRecord, WeakETag.FromVersionId(cosmosExportJobRecord.Resource.ETag));

                return outcome;
            }
            catch (CosmosException dce)
            {
                if (dce.IsRequestRateExceeded())
                {
                    throw;
                }

                if (dce.StatusCode == HttpStatusCode.NotFound)
                {
                    throw new JobNotFoundException(string.Format(Core.Resources.JobNotFound, id));
                }

                _logger.LogError(dce, "Failed to get an export job by id.");
                throw;
            }
        }

        public async Task<ExportJobOutcome> GetExportJobByHashAsync(string hash, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNullOrWhiteSpace(hash, nameof(hash));

            try
            {
                var query = _queryFactory.Create<CosmosExportJobRecordWrapper>(
                    _containerScope.Value,
                    new CosmosQueryContext(
                        new QueryDefinition(GetJobByHashQuery)
                            .WithParameter(HashParameterName, hash),
                        new QueryRequestOptions { PartitionKey = new PartitionKey(CosmosDbExportConstants.ExportJobPartitionKey) }));

                FeedResponse<CosmosExportJobRecordWrapper> result = await query.ExecuteNextAsync();

                if (result.Count == 1)
                {
                    // We found an existing job that matches the hash.
                    CosmosExportJobRecordWrapper wrapper = result.First();

                    return new ExportJobOutcome(wrapper.JobRecord, WeakETag.FromVersionId(wrapper.ETag));
                }

                return null;
            }
            catch (CosmosException dce)
            {
                if (dce.IsRequestRateExceeded())
                {
                    throw;
                }

                _logger.LogError(dce, "Failed to get an export job by hash.");
                throw;
            }
        }

        public async Task<ExportJobOutcome> UpdateExportJobAsync(ExportJobRecord jobRecord, WeakETag eTag, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(jobRecord, nameof(jobRecord));

            var cosmosExportJob = new CosmosExportJobRecordWrapper(jobRecord);
            var requestOptions = new ItemRequestOptions();

            // Create access condition so that record is replaced only if eTag matches.
            if (eTag != null)
            {
                requestOptions.IfMatchEtag = eTag.VersionId;
            }

            try
            {
                var replaceResult = await _retryExceptionPolicyFactory.GetRetryPolicy().ExecuteAsync(
                    () => _containerScope.Value.ReplaceItemAsync(
                        cosmosExportJob,
                        jobRecord.Id,
                        new PartitionKey(CosmosDbExportConstants.ExportJobPartitionKey),
                        cancellationToken: cancellationToken,
                        requestOptions: requestOptions));

                var latestETag = replaceResult.Resource.ETag;
                return new ExportJobOutcome(jobRecord, WeakETag.FromVersionId(latestETag));
            }
            catch (CosmosException dce)
            {
                if (dce.IsRequestRateExceeded())
                {
                    throw;
                }
                else if (dce.StatusCode == HttpStatusCode.PreconditionFailed)
                {
                    throw new JobConflictException();
                }
                else if (dce.StatusCode == HttpStatusCode.NotFound)
                {
                    throw new JobNotFoundException(string.Format(Core.Resources.JobNotFound, jobRecord.Id));
                }

                _logger.LogError(dce, "Failed to update an export job.");
                throw;
            }
        }

        public async Task<IReadOnlyCollection<ExportJobOutcome>> AcquireExportJobsAsync(
            ushort maximumNumberOfConcurrentJobsAllowed,
            TimeSpan jobHeartbeatTimeoutThreshold,
            CancellationToken cancellationToken)
        {
            try
            {
                var response = await _retryExceptionPolicyFactory.GetRetryPolicy().ExecuteAsync(
                    async ct => await _acquireExportJobs.ExecuteAsync(
                        _containerScope.Value.Scripts,
                        maximumNumberOfConcurrentJobsAllowed,
                        (ushort)jobHeartbeatTimeoutThreshold.TotalSeconds,
                        ct),
                    cancellationToken);

                return response.Resource.Select(wrapper => new ExportJobOutcome(wrapper.JobRecord, WeakETag.FromVersionId(wrapper.ETag))).ToList();
            }
            catch (CosmosException dce)
            {
                if (dce.IsRequestEntityTooLarge())
                {
                    throw;
                }

                _logger.LogError(dce, "Failed to acquire export jobs.");
                throw;
            }
        }

        public async Task<ReindexJobWrapper> CreateReindexJobAsync(ReindexJobRecord jobRecord, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(jobRecord, nameof(jobRecord));

            var cosmosReindexJob = new CosmosReindexJobRecordWrapper(jobRecord);

            try
            {
                var result = await _containerScope.Value.CreateItemAsync(
                    cosmosReindexJob,
                    new PartitionKey(CosmosDbReindexConstants.ReindexJobPartitionKey),
                    cancellationToken: cancellationToken);

                return new ReindexJobWrapper(jobRecord, WeakETag.FromVersionId(result.Resource.ETag));
            }
            catch (CosmosException dce)
            {
                if (dce.IsRequestRateExceeded())
                {
                    throw;
                }

                _logger.LogError(dce, "Failed to create a reindex job.");
                throw;
            }
        }

        public async Task<IReadOnlyCollection<ReindexJobWrapper>> AcquireReindexJobsAsync(ushort maximumNumberOfConcurrentJobsAllowed, TimeSpan jobHeartbeatTimeoutThreshold, CancellationToken cancellationToken)
        {
            try
            {
                StoredProcedureExecuteResponse<IReadOnlyCollection<CosmosReindexJobRecordWrapper>> response = await _retryExceptionPolicyFactory.GetRetryPolicy().ExecuteAsync(
                    async ct => await _acquireReindexJobs.ExecuteAsync(
                        _containerScope.Value.Scripts,
                        maximumNumberOfConcurrentJobsAllowed,
                        (ushort)jobHeartbeatTimeoutThreshold.TotalSeconds,
                        ct),
                    cancellationToken);

                return response.Resource.Select(cosmosReindexWrapper => new ReindexJobWrapper(cosmosReindexWrapper.JobRecord, WeakETag.FromVersionId(cosmosReindexWrapper.ETag))).ToList();
            }
            catch (CosmosException dce)
            {
                if (dce.IsRequestEntityTooLarge())
                {
                    throw;
                }

                _logger.LogError(dce, "Failed to acquire reindex jobs.");
                throw;
            }
        }

        public async Task<(bool found, string id)> CheckActiveReindexJobsAsync(CancellationToken cancellationToken)
        {
            try
            {
                var query = _queryFactory.Create<CosmosReindexJobRecordWrapper>(
                    _containerScope.Value,
                    new CosmosQueryContext(
                        new QueryDefinition(CheckActiveJobsByStatusQuery),
                        new QueryRequestOptions { PartitionKey = new PartitionKey(CosmosDbReindexConstants.ReindexJobPartitionKey) }));

                FeedResponse<CosmosReindexJobRecordWrapper> result = await query.ExecuteNextAsync();

                if (result.Any())
                {
                    return (true, result.FirstOrDefault().JobRecord.Id);
                }

                return (false, string.Empty);
            }
            catch (CosmosException dce)
            {
                if (dce.IsRequestRateExceeded())
                {
                    throw;
                }

                _logger.LogError(dce, "Failed to check if any reindex jobs are active.");
                throw;
            }
        }

        public async Task<ReindexJobWrapper> GetReindexJobByIdAsync(string jobId, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNullOrWhiteSpace(jobId, nameof(jobId));

            try
            {
                var cosmosReindexJobRecord = await _containerScope.Value.ReadItemAsync<CosmosReindexJobRecordWrapper>(
                    jobId,
                    new PartitionKey(CosmosDbReindexConstants.ReindexJobPartitionKey),
                    cancellationToken: cancellationToken);

                var outcome = new ReindexJobWrapper(
                    cosmosReindexJobRecord.Resource.JobRecord,
                    WeakETag.FromVersionId(cosmosReindexJobRecord.Resource.ETag));

                return outcome;
            }
            catch (CosmosException dce)
            {
                if (dce.IsRequestRateExceeded())
                {
                    throw;
                }
                else if (dce.StatusCode == HttpStatusCode.NotFound)
                {
                    throw new JobNotFoundException(string.Format(Core.Resources.JobNotFound, jobId));
                }

                _logger.LogError(dce, "Failed to get reindex job by id: {jobId}.", jobId);
                throw;
            }
        }

        public async Task<ReindexJobWrapper> UpdateReindexJobAsync(ReindexJobRecord jobRecord, WeakETag eTag, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(jobRecord, nameof(jobRecord));

            var cosmosReindexJob = new CosmosReindexJobRecordWrapper(jobRecord);
            var requestOptions = new ItemRequestOptions();

            // Create access condition so that record is replaced only if eTag matches.
            if (eTag != null)
            {
                requestOptions.IfMatchEtag = eTag.VersionId;
            }

            try
            {
                var replaceResult = await _retryExceptionPolicyFactory.GetRetryPolicy().ExecuteAsync(
                    () => _containerScope.Value.ReplaceItemAsync(
                        cosmosReindexJob,
                        jobRecord.Id,
                        new PartitionKey(CosmosDbReindexConstants.ReindexJobPartitionKey),
                        requestOptions,
                        cancellationToken));

                var latestETag = replaceResult.Resource.ETag;
                return new ReindexJobWrapper(jobRecord, WeakETag.FromVersionId(latestETag));
            }
            catch (CosmosException dce)
            {
                if (dce.IsRequestRateExceeded())
                {
                    throw;
                }
                else if (dce.StatusCode == HttpStatusCode.PreconditionFailed)
                {
                    throw new JobConflictException();
                }
                else if (dce.StatusCode == HttpStatusCode.NotFound)
                {
                    throw new JobNotFoundException(string.Format(Core.Resources.JobNotFound, jobRecord.Id));
                }

                _logger.LogError(dce, "Failed to update a reindex job.");
                throw;
            }
        }
    }
}
