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
using Hl7.Fhir.ElementModel;
using Hl7.Fhir.Model;
using Hl7.Fhir.Serialization;
using MediatR;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Core.Configs;
using Microsoft.Health.Fhir.Core.Extensions;
using Microsoft.Health.Fhir.Core.Features.Context;
using Microsoft.Health.Fhir.Core.Features.Definition;
using Microsoft.Health.Fhir.Core.Features.Operations;
using Microsoft.Health.Fhir.Core.Features.Operations.Reindex;
using Microsoft.Health.Fhir.Core.Features.Operations.Reindex.Models;
using Microsoft.Health.Fhir.Core.Features.Persistence;
using Microsoft.Health.Fhir.Core.Features.Search;
using Microsoft.Health.Fhir.Core.Features.Search.Parameters;
using Microsoft.Health.Fhir.Core.Features.Search.Registry;
using Microsoft.Health.Fhir.Core.Features.Search.SearchValues;
using Microsoft.Health.Fhir.Core.Features.Security.Authorization;
using Microsoft.Health.Fhir.Core.Messages.Reindex;
using Microsoft.Health.Fhir.Core.Models;
using Microsoft.Health.Fhir.Core.UnitTests.Extensions;
using Microsoft.Health.Fhir.Tests.Common;
using Microsoft.Health.Fhir.Tests.Common.FixtureParameters;
using Microsoft.Health.Fhir.Tests.Integration.Persistence;
using Microsoft.Health.Test.Utilities;
using NSubstitute;
using Xunit;
using Task = System.Threading.Tasks.Task;

namespace Microsoft.Health.Fhir.Tests.Integration.Features.Operations.Reindex
{
    [FhirStorageTestsFixtureArgumentSets(DataStore.CosmosDb)]
    public class ReindexJobTests : IClassFixture<FhirStorageTestsFixture>, IAsyncLifetime
    {
        private readonly FhirStorageTestsFixture _fixture;
        private readonly IFhirStorageTestHelper _testHelper;
        private IFhirOperationDataStore _fhirOperationDataStore;
        private IScoped<IFhirOperationDataStore> _scopedOperationDataStore;
        private IScoped<IFhirDataStore> _scopedDataStore;
        private IFhirStorageTestHelper _fhirStorageTestHelper;
        private SearchParameterDefinitionManager _searchParameterDefinitionManager;

        private ReindexJobConfiguration _jobConfiguration;
        private CreateReindexRequestHandler _createReindexRequestHandler;
        private ReindexUtilities _reindexUtilities;
        private readonly ISearchIndexer _searchIndexer = Substitute.For<ISearchIndexer>();
        private ISupportedSearchParameterDefinitionManager _supportedSearchParameterDefinitionManager;
        private SearchParameterStatusManager _searchParameterStatusManager;
        private readonly ISearchParameterSupportResolver _searchParameterSupportResolver = Substitute.For<ISearchParameterSupportResolver>();

        private ReindexJobWorker _reindexJobWorker;
        private IScoped<ISearchService> _searchService;

        private readonly IReindexJobThrottleController _throttleController = Substitute.For<IReindexJobThrottleController>();
        private readonly IFhirRequestContextAccessor _contextAccessor = Substitute.For<IFhirRequestContextAccessor>();

        public ReindexJobTests(FhirStorageTestsFixture fixture)
        {
            _fixture = fixture;
            _testHelper = _fixture.TestHelper;
        }

        public async Task InitializeAsync()
        {
            _fhirOperationDataStore = _fixture.OperationDataStore;
            _fhirStorageTestHelper = _fixture.TestHelper;
            _scopedOperationDataStore = _fhirOperationDataStore.CreateMockScope();
            _scopedDataStore = _fixture.DataStore.CreateMockScope();

            _jobConfiguration = new ReindexJobConfiguration();
            IOptions<ReindexJobConfiguration> optionsReindexConfig = Substitute.For<IOptions<ReindexJobConfiguration>>();
            optionsReindexConfig.Value.Returns(_jobConfiguration);

            _searchParameterDefinitionManager = _fixture.SearchParameterDefinitionManager;
            _supportedSearchParameterDefinitionManager = _fixture.SupportedSearchParameterDefinitionManager;

            ResourceWrapperFactory wrapperFactory = Mock.TypeWithArguments<ResourceWrapperFactory>(
                new RawResourceFactory(new FhirJsonSerializer()),
                _searchIndexer,
                _searchParameterDefinitionManager,
                Deserializers.ResourceDeserializer);

            _searchParameterStatusManager = _fixture.SearchParameterStatusManager;

            _createReindexRequestHandler = new CreateReindexRequestHandler(
                                                _fhirOperationDataStore,
                                                DisabledFhirAuthorizationService.Instance,
                                                optionsReindexConfig,
                                                _searchParameterDefinitionManager);

            _reindexUtilities = new ReindexUtilities(
                () => _scopedDataStore,
                _searchIndexer,
                Deserializers.ResourceDeserializer,
                _supportedSearchParameterDefinitionManager,
                _searchParameterStatusManager,
                wrapperFactory);

            _searchService = _fixture.SearchService.CreateMockScope();

            await _fhirStorageTestHelper.DeleteAllReindexJobRecordsAsync(CancellationToken.None);

            _throttleController.GetThrottleBasedDelay().Returns(0);
            _throttleController.GetThrottleBatchSize().Returns(100U);
        }

        public Task DisposeAsync()
        {
            return Task.CompletedTask;
        }

        [Fact]
        public async Task GivenLessThanMaximumRunningJobs_WhenCreatingAReindexJob_ThenNewJobShouldBeCreated()
        {
            var request = new CreateReindexRequest();

            CreateReindexResponse response = await _createReindexRequestHandler.Handle(request, CancellationToken.None);

            Assert.NotNull(response);
            Assert.False(string.IsNullOrWhiteSpace(response.Job.JobRecord.Id));
        }

        [Fact]
        public async Task GivenAlreadyRunningJob_WhenCreatingAReindexJob_ThenJobConflictExceptionThrown()
        {
            var request = new CreateReindexRequest();

            CreateReindexResponse response = await _createReindexRequestHandler.Handle(request, CancellationToken.None);

            Assert.NotNull(response);
            Assert.False(string.IsNullOrWhiteSpace(response.Job.JobRecord.Id));

            await Assert.ThrowsAsync<JobConflictException>(() => _createReindexRequestHandler.Handle(request, CancellationToken.None));
        }

        [Fact]
        public async Task GivenNoSupportedSearchParameters_WhenRunningReindexJob_ThenJobIsCanceled()
        {
            var request = new CreateReindexRequest();

            CreateReindexResponse response = await _createReindexRequestHandler.Handle(request, CancellationToken.None);

            Assert.NotNull(response);
            Assert.False(string.IsNullOrWhiteSpace(response.Job.JobRecord.Id));

            _reindexJobWorker = new ReindexJobWorker(
                () => _scopedOperationDataStore,
                Options.Create(_jobConfiguration),
                InitializeReindexJobTask,
                NullLogger<ReindexJobWorker>.Instance);

            var cancellationTokenSource = new CancellationTokenSource();

            try
            {
                await PerformReindexingOperation(response, OperationStatus.Canceled, cancellationTokenSource);
            }
            finally
            {
                cancellationTokenSource.Cancel();
            }
        }

        [Fact]
        public async Task GivenNoMatchingResources_WhenRunningReindexJob_ThenJobIsCompleted()
        {
            var searchParam = _supportedSearchParameterDefinitionManager.GetSearchParameter(new Uri("http://hl7.org/fhir/SearchParameter/Measure-name"));
            searchParam.IsSearchable = false;
            var request = new CreateReindexRequest();

            CreateReindexResponse response = await _createReindexRequestHandler.Handle(request, CancellationToken.None);

            Assert.NotNull(response);
            Assert.False(string.IsNullOrWhiteSpace(response.Job.JobRecord.Id));

            _reindexJobWorker = new ReindexJobWorker(
                () => _scopedOperationDataStore,
                Options.Create(_jobConfiguration),
                InitializeReindexJobTask,
                NullLogger<ReindexJobWorker>.Instance);

            var cancellationTokenSource = new CancellationTokenSource();

            try
            {
                await PerformReindexingOperation(response, OperationStatus.Completed, cancellationTokenSource);

                Assert.True(searchParam.IsSearchable);
            }
            finally
            {
                cancellationTokenSource.Cancel();
                searchParam.IsSearchable = true;
            }
        }

        [Fact]
        public async Task GivenNewSearchParamCreatedBeforeResourcesToBeIndexed_WhenReindexJobCompleted_ThenResourcesAreIndexedAndParamIsSearchable()
        {
            const string searchParamName = "foo";
            const string searchParamCode = "fooCode";
            SearchParameter searchParam = await CreateSearchParam(searchParamName, SearchParamType.String, ResourceType.Patient, "Patient.name", searchParamCode);

            const string sampleName1 = "searchIndicesPatient1";
            const string sampleName2 = "searchIndicesPatient2";

            string sampleId1 = Guid.NewGuid().ToString();
            string sampleId2 = Guid.NewGuid().ToString();

            // Set up the values that the search index extraction should return during reindexing
            var searchValues = new List<(string, ISearchValue)> { (sampleId1, new StringSearchValue(sampleName1)), (sampleId2, new StringSearchValue(sampleName2)) };
            MockSearchIndexExtraction(searchValues, searchParam);

            UpsertOutcome sample1 = await CreatePatientResource(sampleName1, sampleId1);
            UpsertOutcome sample2 = await CreatePatientResource(sampleName2, sampleId2);

            // Create the query <fhirserver>/Patient?foo=searchIndicesPatient1
            var queryParams = new List<Tuple<string, string>> { new(searchParamCode, sampleName1) };
            SearchResult searchResults = await _searchService.Value.SearchAsync("Patient", queryParams, CancellationToken.None);

            // Confirm that the search parameter "foo" is marked as unsupported
            Assert.Equal(searchParamCode, searchResults.UnsupportedSearchParameters.FirstOrDefault()?.Item1);

            // When search parameters aren't recognized, they are ignored
            // Confirm that "foo" is dropped from the query string and all patients are returned
            Assert.Equal(2, searchResults.Results.Count());

            CreateReindexResponse response = await SetUpForReindexing();

            var cancellationTokenSource = new CancellationTokenSource();

            try
            {
                await PerformReindexingOperation(response, OperationStatus.Completed, cancellationTokenSource);

                // Rerun the same search as above
                searchResults = await _searchService.Value.SearchAsync("Patient", queryParams, CancellationToken.None);

                // This time, foo should not be dropped from the query string
                Assert.Single(searchResults.Results);

                // The foo search parameter can be used to filter for the first test patient
                ResourceWrapper patient = searchResults.Results.FirstOrDefault().Resource;
                Assert.Contains(sampleName1, patient.RawResource.Data);

                // Confirm that the reindexing operation did not create a new version of the resource
                Assert.Equal("1", searchResults.Results.FirstOrDefault().Resource.Version);
            }
            finally
            {
                cancellationTokenSource.Cancel();

                _searchParameterDefinitionManager.DeleteSearchParameter(searchParam.ToTypedElement());
                await _testHelper.DeleteSearchParameterStatusAsync(searchParam.Url, CancellationToken.None);

                await _fixture.DataStore.HardDeleteAsync(sample1.Wrapper.ToResourceKey(), CancellationToken.None);
                await _fixture.DataStore.HardDeleteAsync(sample2.Wrapper.ToResourceKey(), CancellationToken.None);
            }
        }

        [Fact]
        public async Task GivenNewSearchParamCreatedAfterResourcesToBeIndexed_WhenReindexJobCompleted_ThenResourcesAreIndexedAndParamIsSearchable()
        {
            const string sampleName1 = "searchIndicesPatient1";
            const string sampleName2 = "searchIndicesPatient2";

            string sampleId1 = Guid.NewGuid().ToString();
            string sampleId2 = Guid.NewGuid().ToString();

            UpsertOutcome sample1 = await CreatePatientResource(sampleName1, sampleId1);
            UpsertOutcome sample2 = await CreatePatientResource(sampleName2, sampleId2);

            const string searchParamName = "foo";
            const string searchParamCode = "fooCode";
            SearchParameter searchParam = await CreateSearchParam(searchParamName, SearchParamType.String, ResourceType.Patient, "Patient.name", searchParamCode);

            // Create the query <fhirserver>/Patient?foo=searchIndicesPatient1
            var queryParams = new List<Tuple<string, string>> { new(searchParamCode, sampleName1) };
            SearchResult searchResults = await _searchService.Value.SearchAsync("Patient", queryParams, CancellationToken.None);

            // Confirm that the search parameter "foo" is marked as unsupported
            Assert.Equal(searchParamCode, searchResults.UnsupportedSearchParameters.FirstOrDefault()?.Item1);

            // When search parameters aren't recognized, they are ignored
            // Confirm that "foo" is dropped from the query string and all patients are returned
            Assert.Equal(2, searchResults.Results.Count());

            // Set up the values that the search index extraction should return during reindexing
            var searchValues = new List<(string, ISearchValue)> { (sampleId1, new StringSearchValue(sampleName1)), (sampleId2, new StringSearchValue(sampleName2)) };
            MockSearchIndexExtraction(searchValues, searchParam);

            CreateReindexResponse response = await SetUpForReindexing();

            var cancellationTokenSource = new CancellationTokenSource();

            try
            {
                await PerformReindexingOperation(response, OperationStatus.Completed, cancellationTokenSource);

                // Rerun the same search as above
                searchResults = await _searchService.Value.SearchAsync("Patient", queryParams, CancellationToken.None);

                // This time, foo should not be dropped from the query string
                Assert.Single(searchResults.Results);

                // The foo search parameter can be used to filter for the first test patient
                ResourceWrapper patient = searchResults.Results.FirstOrDefault().Resource;
                Assert.Contains(sampleName1, patient.RawResource.Data);

                // Confirm that the reindexing operation did not create a new version of the resource
                Assert.Equal("1", searchResults.Results.FirstOrDefault().Resource.Version);
            }
            finally
            {
                cancellationTokenSource.Cancel();

                _searchParameterDefinitionManager.DeleteSearchParameter(searchParam.ToTypedElement());
                await _testHelper.DeleteSearchParameterStatusAsync(searchParam.Url, CancellationToken.None);

                await _fixture.DataStore.HardDeleteAsync(sample1.Wrapper.ToResourceKey(), CancellationToken.None);
                await _fixture.DataStore.HardDeleteAsync(sample2.Wrapper.ToResourceKey(), CancellationToken.None);
            }
        }

        [Fact]
        public async Task GivenNewSearchParamWithResourceBaseType_WhenReindexJobCompleted_ThenAllResourcesAreIndexedAndParamIsSearchable()
        {
            string patientId = Guid.NewGuid().ToString();
            string observationId = Guid.NewGuid().ToString();

            UpsertOutcome samplePatient = await CreatePatientResource("samplePatient", patientId);
            UpsertOutcome sampleObservation = await CreateObservationResource(observationId);

            const string searchParamName = "resourceFoo";
            const string searchParamCode = "resourceFooCode";
            SearchParameter searchParam = await CreateSearchParam(searchParamName, SearchParamType.Token, ResourceType.Resource, "Resource.id", searchParamCode);

            // Create the query <fhirserver>/Patient?resourceFooCode=<patientId>
            var queryParams = new List<Tuple<string, string>> { new(searchParamCode, patientId) };
            SearchResult searchResults = await _searchService.Value.SearchAsync("Patient", queryParams, CancellationToken.None);

            // Confirm that the search parameter "resourceFoo" is marked as unsupported
            Assert.Equal(searchParamCode, searchResults.UnsupportedSearchParameters.FirstOrDefault()?.Item1);

            // Set up the values that the search index extraction should return during reindexing
            var searchValues = new List<(string, ISearchValue)> { (patientId, new TokenSearchValue(null, patientId, null)), (observationId, new TokenSearchValue(null, observationId, null)) };
            MockSearchIndexExtraction(searchValues, searchParam);

            CreateReindexResponse response = await SetUpForReindexing();

            var cancellationTokenSource = new CancellationTokenSource();

            try
            {
                await PerformReindexingOperation(response, OperationStatus.Completed, cancellationTokenSource);

                // Rerun the same search as above
                searchResults = await _searchService.Value.SearchAsync("Patient", queryParams, CancellationToken.None);
                Assert.Single(searchResults.Results);

                // Confirm that the search parameter "resourceFoo" isn't marked as unsupported
                Assert.DoesNotContain(searchResults.UnsupportedSearchParameters, t => t.Item1 == searchParamCode);

                // Create the query <fhirserver>/Patient?resourceFooCode=<nonexistent-id>
                queryParams = new List<Tuple<string, string>> { new(searchParamCode, "nonexistent-id") };

                // No resources should be returned
                searchResults = await _searchService.Value.SearchAsync("Patient", queryParams, CancellationToken.None);
                Assert.Empty(searchResults.Results);

                // Create the query <fhirserver>/Observation?resourceFooCode=<observationId>
                queryParams = new List<Tuple<string, string>> { new(searchParamCode, observationId) };

                // Check that the new search parameter can be used with a different type of resource
                searchResults = await _searchService.Value.SearchAsync("Observation", queryParams, CancellationToken.None);
                Assert.Single(searchResults.Results);

                // Confirm that the search parameter "resourceFoo" isn't marked as unsupported
                Assert.DoesNotContain(searchResults.UnsupportedSearchParameters, t => t.Item1 == searchParamCode);

                // Create the query <fhirserver>/Observation?resourceFooCode=<nonexistent-id>
                queryParams = new List<Tuple<string, string>> { new(searchParamCode, "nonexistent-id") };

                // No resources should be returned
                searchResults = await _searchService.Value.SearchAsync("Observation", queryParams, CancellationToken.None);
                Assert.Empty(searchResults.Results);
            }
            finally
            {
                cancellationTokenSource.Cancel();

                _searchParameterDefinitionManager.DeleteSearchParameter(searchParam.ToTypedElement());
                await _testHelper.DeleteSearchParameterStatusAsync(searchParam.Url, CancellationToken.None);

                await _fixture.DataStore.HardDeleteAsync(samplePatient.Wrapper.ToResourceKey(), CancellationToken.None);
                await _fixture.DataStore.HardDeleteAsync(sampleObservation.Wrapper.ToResourceKey(), CancellationToken.None);
            }
        }

        private async Task PerformReindexingOperation(CreateReindexResponse response, OperationStatus operationStatus, CancellationTokenSource cancellationTokenSource)
        {
            Task reindexWorkerTask = _reindexJobWorker.ExecuteAsync(cancellationTokenSource.Token);
            ReindexJobWrapper reindexJobWrapper = await _fhirOperationDataStore.GetReindexJobByIdAsync(response.Job.JobRecord.Id, cancellationTokenSource.Token);

            int delayCount = 0;

            while (reindexJobWrapper.JobRecord.Status != operationStatus && delayCount < 40)
            {
                await Task.Delay(1000);
                delayCount++;
                reindexJobWrapper = await _fhirOperationDataStore.GetReindexJobByIdAsync(response.Job.JobRecord.Id, cancellationTokenSource.Token);
            }

            Assert.Equal(operationStatus, reindexJobWrapper.JobRecord.Status);
        }

        private async Task<CreateReindexResponse> SetUpForReindexing()
        {
            var request = new CreateReindexRequest();

            CreateReindexResponse response = await _createReindexRequestHandler.Handle(request, CancellationToken.None);

            Assert.NotNull(response);
            Assert.False(string.IsNullOrWhiteSpace(response.Job.JobRecord.Id));

            _reindexJobWorker = new ReindexJobWorker(
                () => _scopedOperationDataStore,
                Options.Create(_jobConfiguration),
                InitializeReindexJobTask,
                NullLogger<ReindexJobWorker>.Instance);
            return response;
        }

        private void MockSearchIndexExtraction(IEnumerable<(string id, ISearchValue searchValue)> searchValues, SearchParameter searchParam)
        {
            SearchParameterInfo searchParamInfo = searchParam.ToInfo();

            foreach ((string id, ISearchValue searchValue) in searchValues)
            {
                var searchIndexValues = new List<SearchIndexEntry>();
                searchIndexValues.Add(new SearchIndexEntry(searchParamInfo, searchValue));
                _searchIndexer.Extract(Arg.Is<ResourceElement>(r => r.Id.Equals(id))).Returns(searchIndexValues);
            }
        }

        private async Task<SearchParameter> CreateSearchParam(string searchParamName, SearchParamType searchParamType, ResourceType baseType, string expression, string searchParamCode)
        {
            var searchParam = new SearchParameter
            {
                Url = $"http://hl7.org/fhir/SearchParameter/{baseType}-{searchParamName}",
                Type = searchParamType,
                Base = new List<ResourceType?> { baseType },
                Expression = expression,
                Name = searchParamName,
                Code = searchParamCode,
            };

            _searchParameterDefinitionManager.AddNewSearchParameters(new List<ITypedElement> { searchParam.ToTypedElement() });

            // Add the search parameter to the datastore
            await _searchParameterStatusManager.UpdateSearchParameterStatusAsync(new List<string> { searchParam.Url }, SearchParameterStatus.Supported);

            return searchParam;
        }

        private ReindexJobTask InitializeReindexJobTask()
        {
            return new ReindexJobTask(
                () => _scopedOperationDataStore,
                () => _scopedDataStore,
                Options.Create(_jobConfiguration),
                () => _searchService,
                _supportedSearchParameterDefinitionManager,
                _reindexUtilities,
                _contextAccessor,
                _throttleController,
                ModelInfoProvider.Instance,
                NullLogger<ReindexJobTask>.Instance);
        }

        private ResourceWrapper CreatePatientResourceWrapper(string patientName, string patientId)
        {
            Patient patientResource = Samples.GetDefaultPatient().ToPoco<Patient>();

            patientResource.Name = new List<HumanName> { new() { Family = patientName }};
            patientResource.Id = patientId;
            patientResource.VersionId = "1";

            var resourceElement = patientResource.ToResourceElement();
            var rawResource = new RawResource(patientResource.ToJson(), FhirResourceFormat.Json, isMetaSet: false);
            var resourceRequest = new ResourceRequest(WebRequestMethods.Http.Put);
            var compartmentIndices = Substitute.For<CompartmentIndices>();
            var searchIndices = _searchIndexer.Extract(resourceElement);
            var wrapper = new ResourceWrapper(resourceElement, rawResource, resourceRequest, false, searchIndices, compartmentIndices, new List<KeyValuePair<string, string>>(), _searchParameterDefinitionManager.GetSearchParameterHashForResourceType("Patient"));

            return wrapper;
        }

        private ResourceWrapper CreateObservationResourceWrapper(string observationId)
        {
            Observation observationResource = Samples.GetDefaultObservation().ToPoco<Observation>();

            observationResource.Id = observationId;
            observationResource.VersionId = "1";

            var resourceElement = observationResource.ToResourceElement();
            var rawResource = new RawResource(observationResource.ToJson(), FhirResourceFormat.Json, isMetaSet: false);
            var resourceRequest = new ResourceRequest(WebRequestMethods.Http.Put);
            var compartmentIndices = Substitute.For<CompartmentIndices>();
            var searchIndices = _searchIndexer.Extract(resourceElement);
            var wrapper = new ResourceWrapper(resourceElement, rawResource, resourceRequest, false, searchIndices, compartmentIndices, new List<KeyValuePair<string, string>>(), _searchParameterDefinitionManager.GetSearchParameterHashForResourceType("Observation"));

            return wrapper;
        }

        private async Task<UpsertOutcome> CreatePatientResource(string patientName, string patientId)
        {
            return await _scopedDataStore.Value.UpsertAsync(CreatePatientResourceWrapper(patientName, patientId), null, true, true, CancellationToken.None);
        }

        private async Task<UpsertOutcome> CreateObservationResource(string observationId)
        {
            return await _scopedDataStore.Value.UpsertAsync(CreateObservationResourceWrapper(observationId), null, true, true, CancellationToken.None);
        }
    }
}
