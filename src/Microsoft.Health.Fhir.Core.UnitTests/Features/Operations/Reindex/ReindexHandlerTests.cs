﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Abstractions.Exceptions;
using Microsoft.Health.Core.Features.Security.Authorization;
using Microsoft.Health.Fhir.Core.Configs;
using Microsoft.Health.Fhir.Core.Exceptions;
using Microsoft.Health.Fhir.Core.Features.Operations;
using Microsoft.Health.Fhir.Core.Features.Operations.Reindex;
using Microsoft.Health.Fhir.Core.Features.Operations.Reindex.Models;
using Microsoft.Health.Fhir.Core.Features.Persistence;
using Microsoft.Health.Fhir.Core.Features.Security;
using Microsoft.Health.Fhir.Core.Features.Security.Authorization;
using Microsoft.Health.Fhir.Core.Messages.Reindex;
using Microsoft.Health.Fhir.Core.UnitTests.Extensions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Microsoft.Health.Fhir.Core.UnitTests.Features.Operations.Reindex
{
    public class ReindexHandlerTests
    {
        private readonly IFhirOperationDataStore _fhirOperationDataStore = Substitute.For<IFhirOperationDataStore>();
        private IReadOnlyDictionary<string, string> _resourceTypeSearchParameterHashMap;
        private readonly ReindexJobConfiguration _reindexJobConfiguration = new ReindexJobConfiguration();
        private readonly Func<IReindexJobTask> _reindexJobTaskFactory = Substitute.For<Func<IReindexJobTask>>();

        public ReindexHandlerTests()
        {
            _resourceTypeSearchParameterHashMap = new Dictionary<string, string>() { { "resourceType", "paramHash" } };
        }

        [Fact]
        public async Task GivenAGetRequest_WhenGettingAnExistingJob_ThenHttpResponseCodeShouldBeOk()
        {
            var request = new GetReindexRequest("id");

            var jobRecord = new ReindexJobRecord(_resourceTypeSearchParameterHashMap, 1);
            var jobWrapper = new ReindexJobWrapper(jobRecord, WeakETag.FromVersionId("id"));
            _fhirOperationDataStore.GetReindexJobByIdAsync("id", Arg.Any<CancellationToken>()).Returns(jobWrapper);

            var handler = new GetReindexRequestHandler(_fhirOperationDataStore, DisabledFhirAuthorizationService.Instance);

            var result = await handler.Handle(request, CancellationToken.None);

            Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        }

        [Fact]
        public async Task GivenAGetRequest_WhenUserUnauthorized_ThenUnauthorizedFhirExceptionThrown()
        {
            var request = new GetReindexRequest("id");

            var jobRecord = new ReindexJobRecord(_resourceTypeSearchParameterHashMap, 1);
            var jobWrapper = new ReindexJobWrapper(jobRecord, WeakETag.FromVersionId("id"));
            _fhirOperationDataStore.GetReindexJobByIdAsync("id", Arg.Any<CancellationToken>()).Returns(jobWrapper);

            var authorizationService = Substitute.For<IAuthorizationService<DataActions>>();
            authorizationService.CheckAccess(DataActions.Reindex, Arg.Any<CancellationToken>()).Returns(DataActions.None);

            var handler = new GetReindexRequestHandler(_fhirOperationDataStore, authorizationService);

            await Assert.ThrowsAsync<UnauthorizedFhirActionException>(() => handler.Handle(request, CancellationToken.None));
        }

        [Fact]
        public async Task GivenAGetRequest_WhenIdNotFound_ThenJobNotFoundExceptionThrown()
        {
            var request = new GetReindexRequest("id");

            var jobRecord = new ReindexJobRecord(_resourceTypeSearchParameterHashMap, 1);
            var jobWrapper = new ReindexJobWrapper(jobRecord, WeakETag.FromVersionId("id"));
            _fhirOperationDataStore.GetReindexJobByIdAsync("id", Arg.Any<CancellationToken>()).Throws(new JobNotFoundException("not found"));

            var handler = new GetReindexRequestHandler(_fhirOperationDataStore, DisabledFhirAuthorizationService.Instance);

            await Assert.ThrowsAsync<JobNotFoundException>(() => handler.Handle(request, CancellationToken.None));
        }

        [Fact]
        public async Task GivenAGetRequest_WhenTooManyRequestsThrown_ThenTooManyRequestsThrown()
        {
            var request = new GetReindexRequest("id");

            var jobRecord = new ReindexJobRecord(_resourceTypeSearchParameterHashMap, 1);
            var jobWrapper = new ReindexJobWrapper(jobRecord, WeakETag.FromVersionId("id"));
            _fhirOperationDataStore.GetReindexJobByIdAsync("id", CancellationToken.None).Throws(new Exception(null, new RequestRateExceededException(TimeSpan.FromMilliseconds(100))));

            var handler = new GetReindexRequestHandler(_fhirOperationDataStore, DisabledFhirAuthorizationService.Instance);

            Exception thrownException = await Assert.ThrowsAsync<Exception>(() => handler.Handle(request, CancellationToken.None));
            Assert.IsType<RequestRateExceededException>(thrownException.InnerException);
        }

        [Fact]
        public async Task GivenACancelRequest_WhenUserUnauthorized_ThenUnauthorizedFhirExceptionThrown()
        {
            var request = new CancelReindexRequest("id");

            var jobRecord = new ReindexJobRecord(_resourceTypeSearchParameterHashMap, 1);
            var jobWrapper = new ReindexJobWrapper(jobRecord, WeakETag.FromVersionId("id"));
            _fhirOperationDataStore.GetReindexJobByIdAsync("id", Arg.Any<CancellationToken>()).Returns(jobWrapper);

            var authorizationService = Substitute.For<IAuthorizationService<DataActions>>();
            authorizationService.CheckAccess(DataActions.Reindex, Arg.Any<CancellationToken>()).Returns(DataActions.None);

            var handler = CreateReindexJobWorker(authorizationService);

            await Assert.ThrowsAsync<UnauthorizedFhirActionException>(() => handler.Handle(request, CancellationToken.None));
        }

        [Fact]
        public async Task GivenACancelRequest_WhenJobCompleted_ThenRequestNotValidExceptionThrown()
        {
            var request = new CancelReindexRequest("id");

            var jobRecord = new ReindexJobRecord(_resourceTypeSearchParameterHashMap, 1)
            {
                Status = OperationStatus.Completed,
            };

            var jobWrapper = new ReindexJobWrapper(jobRecord, WeakETag.FromVersionId("id"));
            _fhirOperationDataStore.GetReindexJobByIdAsync("id", Arg.Any<CancellationToken>()).Returns(jobWrapper);

            var handler = CreateReindexJobWorker(DisabledFhirAuthorizationService.Instance);

            await Assert.ThrowsAsync<RequestNotValidException>(() => handler.Handle(request, CancellationToken.None));
        }

        [Fact]
        public async Task GivenACancelRequest_WhenJobInProgress_ThenJobUpdatedToCanceled()
        {
            var jobRecord = new ReindexJobRecord(_resourceTypeSearchParameterHashMap, 1)
            {
                Status = OperationStatus.Running,
            };

            var request = new CancelReindexRequest(jobRecord.Id);

            var jobWrapper = new ReindexJobWrapper(jobRecord, WeakETag.FromVersionId(jobRecord.Id));
            _fhirOperationDataStore.GetReindexJobByIdAsync(jobRecord.Id, Arg.Any<CancellationToken>()).Returns(jobWrapper);
            _fhirOperationDataStore.AcquireReindexJobsAsync(Arg.Any<ushort>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>()).Returns(new List<ReindexJobWrapper>() { jobWrapper });
            _fhirOperationDataStore.UpdateReindexJobAsync(jobRecord, WeakETag.FromVersionId(jobRecord.Id), Arg.Any<CancellationToken>()).Returns(jobWrapper);

            var handler = CreateReindexJobWorker(DisabledFhirAuthorizationService.Instance);

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            handler.ExecuteAsync(CancellationToken.None);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

            await Task.Delay(100);
            var result = await handler.Handle(request, CancellationToken.None);

            Assert.Equal(OperationStatus.Canceled, result.Job.JobRecord.Status);
        }

        private ReindexJobWorker CreateReindexJobWorker(IAuthorizationService<DataActions> authorizationService)
        {
            return new ReindexJobWorker(
                () => _fhirOperationDataStore.CreateMockScope(),
                Options.Create(_reindexJobConfiguration),
                _reindexJobTaskFactory,
                authorizationService,
                NullLogger<ReindexJobWorker>.Instance);
        }
    }
}
