﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Abstractions.Exceptions;
using Microsoft.Health.Core;
using Microsoft.Health.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Core.Configs;
using Microsoft.Health.Fhir.Core.Exceptions;
using Microsoft.Health.Fhir.Core.Extensions;
using Microsoft.Health.Fhir.Core.Features.Conformance;
using Microsoft.Health.Fhir.Core.Features.Definition;
using Microsoft.Health.Fhir.Core.Features.Persistence;
using Microsoft.Health.Fhir.Core.Models;
using Microsoft.Health.Fhir.CosmosDb.Configs;
using Microsoft.Health.Fhir.CosmosDb.Features.Queries;
using Microsoft.Health.Fhir.CosmosDb.Features.Search;
using Microsoft.Health.Fhir.CosmosDb.Features.Storage.StoredProcedures.HardDelete;
using Microsoft.Health.Fhir.CosmosDb.Features.Storage.StoredProcedures.Replace;
using Microsoft.Health.Fhir.ValueSets;
using Microsoft.IO;
using Polly;
using Task = System.Threading.Tasks.Task;

namespace Microsoft.Health.Fhir.CosmosDb.Features.Storage
{
    public sealed class CosmosFhirDataStore : IFhirDataStore, IProvideCapability
    {
        /// <summary>
        /// The fraction of <see cref="QueryRequestOptions.MaxItemCount"/> to attempt to fill before giving up.
        /// </summary>
        internal const double ExecuteDocumentQueryAsyncMinimumFillFactor = 0.5;

        internal const double ExecuteDocumentQueryAsyncMaximumFillFactor = 10;

        private readonly IScoped<Container> _containerScope;
        private readonly CosmosDataStoreConfiguration _cosmosDataStoreConfiguration;
        private readonly ICosmosQueryFactory _cosmosQueryFactory;
        private readonly RetryExceptionPolicyFactory _retryExceptionPolicyFactory;
        private readonly ILogger<CosmosFhirDataStore> _logger;
        private readonly Lazy<ISupportedSearchParameterDefinitionManager> _supportedSearchParameters;

        private static readonly HardDelete _hardDelete = new HardDelete();
        private static readonly ReplaceSingleResource _replaceSingleResource = new ReplaceSingleResource();
        private static readonly RecyclableMemoryStreamManager _recyclableMemoryStreamManager = new();
        private readonly CoreFeatureConfiguration _coreFeatures;

        /// <summary>
        /// This is the maximum degree of parallelism for the SDK to use when querying physical partitions in parallel.
        /// -1 means "System Decides", int.MaxValue sets the limit high enough that it shouldn't be limited.
        /// </summary>
        internal const int MaxQueryConcurrency = int.MaxValue;

        /// <summary>
        /// Initializes a new instance of the <see cref="CosmosFhirDataStore"/> class.
        /// </summary>
        /// <param name="containerScope">
        /// A function that returns an <see cref="Container"/>.
        /// Note that this is a function so that the lifetime of the instance is not directly controlled by the IoC container.
        /// </param>
        /// <param name="cosmosDataStoreConfiguration">The data store configuration.</param>
        /// <param name="namedCosmosCollectionConfigurationAccessor">The IOptions accessor to get a named version.</param>
        /// <param name="cosmosQueryFactory">The factory used to create the document query.</param>
        /// <param name="retryExceptionPolicyFactory">The retry exception policy factory.</param>
        /// <param name="logger">The logger instance.</param>
        /// <param name="coreFeatures">The core feature configuration</param>
        /// <param name="supportedSearchParameters">The supported search parameters</param>
        public CosmosFhirDataStore(
            IScoped<Container> containerScope,
            CosmosDataStoreConfiguration cosmosDataStoreConfiguration,
            IOptionsMonitor<CosmosCollectionConfiguration> namedCosmosCollectionConfigurationAccessor,
            ICosmosQueryFactory cosmosQueryFactory,
            RetryExceptionPolicyFactory retryExceptionPolicyFactory,
            ILogger<CosmosFhirDataStore> logger,
            IOptions<CoreFeatureConfiguration> coreFeatures,
            Lazy<ISupportedSearchParameterDefinitionManager> supportedSearchParameters)
        {
            EnsureArg.IsNotNull(containerScope, nameof(containerScope));
            EnsureArg.IsNotNull(cosmosDataStoreConfiguration, nameof(cosmosDataStoreConfiguration));
            EnsureArg.IsNotNull(namedCosmosCollectionConfigurationAccessor, nameof(namedCosmosCollectionConfigurationAccessor));
            EnsureArg.IsNotNull(cosmosQueryFactory, nameof(cosmosQueryFactory));
            EnsureArg.IsNotNull(retryExceptionPolicyFactory, nameof(retryExceptionPolicyFactory));
            EnsureArg.IsNotNull(logger, nameof(logger));
            EnsureArg.IsNotNull(coreFeatures, nameof(coreFeatures));
            EnsureArg.IsNotNull(supportedSearchParameters, nameof(supportedSearchParameters));

            _containerScope = containerScope;
            _cosmosDataStoreConfiguration = cosmosDataStoreConfiguration;
            _cosmosQueryFactory = cosmosQueryFactory;
            _retryExceptionPolicyFactory = retryExceptionPolicyFactory;
            _logger = logger;
            _supportedSearchParameters = supportedSearchParameters;
            _coreFeatures = coreFeatures.Value;
        }

        public async Task<UpsertOutcome> UpsertAsync(
            ResourceWrapper resource,
            WeakETag weakETag,
            bool allowCreate,
            bool keepHistory,
            CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(resource, nameof(resource));

            var cosmosWrapper = new FhirCosmosResourceWrapper(resource);
            UpdateSortIndex(cosmosWrapper);

            var partitionKey = new PartitionKey(cosmosWrapper.PartitionKey);
            AsyncPolicy retryPolicy = _retryExceptionPolicyFactory.GetRetryPolicy();

            _logger.LogDebug("Upserting {resourceType}/{resourceId}, ETag: \"{tag}\", AllowCreate: {allowCreate}, KeepHistory: {keepHistory}", resource.ResourceTypeName, resource.ResourceId, weakETag?.VersionId, allowCreate, keepHistory);

            if (weakETag == null && allowCreate && !cosmosWrapper.IsDeleted)
            {
                // Optimistically try to create this as a new resource
                try
                {
                    await retryPolicy.ExecuteAsync(
                        async ct => await _containerScope.Value.CreateItemAsync(
                            cosmosWrapper,
                            partitionKey,
                            cancellationToken: ct,
                            requestOptions: new ItemRequestOptions { EnableContentResponseOnWrite = false }),
                        cancellationToken);

                    return new UpsertOutcome(cosmosWrapper, SaveOutcomeType.Created);
                }
                catch (CosmosException e) when (e.StatusCode == HttpStatusCode.Conflict)
                {
                    // this means there is already an existing version of this resource
                }
            }

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                FhirCosmosResourceWrapper existingItemResource;
                try
                {
                    ItemResponse<FhirCosmosResourceWrapper> existingItem = await retryPolicy.ExecuteAsync(
                        async ct => await _containerScope.Value.ReadItemAsync<FhirCosmosResourceWrapper>(cosmosWrapper.Id, partitionKey, cancellationToken: ct),
                        cancellationToken);
                    existingItemResource = existingItem.Resource;
                }
                catch (CosmosException e) when (e.StatusCode == HttpStatusCode.NotFound)
                {
                    if (cosmosWrapper.IsDeleted)
                    {
                        return null;
                    }

                    if (weakETag != null)
                    {
                        throw new ResourceNotFoundException(string.Format(Core.Resources.ResourceNotFoundByIdAndVersion, resource.ResourceTypeName, resource.ResourceId, weakETag.VersionId));
                    }

                    if (!allowCreate)
                    {
                        throw new MethodNotAllowedException(Core.Resources.ResourceCreationNotAllowed);
                    }

                    throw;
                }

                if (weakETag != null && weakETag.VersionId != existingItemResource.Version)
                {
                    throw new PreconditionFailedException(string.Format(Core.Resources.ResourceVersionConflict, weakETag.VersionId));
                }

                if (existingItemResource.IsDeleted && cosmosWrapper.IsDeleted)
                {
                    return null;
                }

                cosmosWrapper.Version = int.TryParse(existingItemResource.Version, out int existingVersion) ? (existingVersion + 1).ToString(CultureInfo.InvariantCulture) : Guid.NewGuid().ToString();

                // indicate that the version in the raw resource's meta property does not reflect the actual version.
                cosmosWrapper.RawResource.IsMetaSet = false;

                if (cosmosWrapper.RawResource.Format == FhirResourceFormat.Json)
                {
                    // Update the raw resource based on the new version.
                    // This is a lot faster than re-serializing the POCO.
                    // Unfortunately, we need to allocate a string, but at least it is reused for the HTTP response.

                    // If the format is not XML, IsMetaSet will remain false and we will update the version when the resource is read.

                    using MemoryStream memoryStream = _recyclableMemoryStreamManager.GetStream();
                    await new RawResourceElement(cosmosWrapper).SerializeToStreamAsUtf8Json(memoryStream);
                    memoryStream.Position = 0;
                    using var reader = new StreamReader(memoryStream, Encoding.UTF8);
                    cosmosWrapper.RawResource = new RawResource(reader.ReadToEnd(), FhirResourceFormat.Json, isMetaSet: true);
                }

                if (keepHistory)
                {
                    existingItemResource.IsHistory = true;
                    existingItemResource.ActivePeriodEndDateTime = cosmosWrapper.LastModified;
                    existingItemResource.SearchIndices = null;

                    TransactionalBatchResponse transactionalBatchResponse = await retryPolicy.ExecuteAsync(
                        async ct =>
                            await _containerScope.Value.CreateTransactionalBatch(partitionKey)
                                .ReplaceItem(cosmosWrapper.Id, cosmosWrapper, new TransactionalBatchItemRequestOptions { EnableContentResponseOnWrite = false, IfMatchEtag = existingItemResource.ETag })
                                .CreateItem(existingItemResource, new TransactionalBatchItemRequestOptions { EnableContentResponseOnWrite = false })
                                .ExecuteAsync(cancellationToken: ct),
                        cancellationToken);

                    if (!transactionalBatchResponse.IsSuccessStatusCode)
                    {
                        if (transactionalBatchResponse.StatusCode == HttpStatusCode.PreconditionFailed)
                        {
                            // someone else beat us to it, re-read and try again
                            continue;
                        }

                        throw new InvalidOperationException(transactionalBatchResponse.ErrorMessage);
                    }
                }
                else
                {
                    try
                    {
                        await retryPolicy.ExecuteAsync(
                            async ct => await _containerScope.Value.ReplaceItemAsync(
                                cosmosWrapper,
                                cosmosWrapper.Id,
                                partitionKey,
                                new ItemRequestOptions { EnableContentResponseOnWrite = false, IfMatchEtag = existingItemResource.ETag },
                                cancellationToken: ct),
                            cancellationToken);
                    }
                    catch (CosmosException e) when (e.StatusCode == HttpStatusCode.PreconditionFailed)
                    {
                        // someone else beat us to it, re-read and try again
                        continue;
                    }
                }

                return new UpsertOutcome(cosmosWrapper, SaveOutcomeType.Updated);
            }
        }

        public async Task<ResourceWrapper> GetAsync(ResourceKey key, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(key, nameof(key));

            bool isVersionedRead = !string.IsNullOrEmpty(key.VersionId);

            if (isVersionedRead)
            {
                QueryDefinition sqlQuerySpec = new QueryDefinition($"select {SearchValueConstants.SelectedFields} from root r where r.resourceId = @resourceId and r.version = @version")
                    .WithParameter("@resourceId", key.Id)
                    .WithParameter("@version", key.VersionId);

                (IReadOnlyList<FhirCosmosResourceWrapper> results, _) = await _retryExceptionPolicyFactory.GetRetryPolicy().ExecuteAsync(() =>
                    ExecuteDocumentQueryAsync<FhirCosmosResourceWrapper>(
                        sqlQuerySpec,
                        new QueryRequestOptions { PartitionKey = new PartitionKey(key.ToPartitionKey()) },
                        cancellationToken: cancellationToken));

                return results.Count == 0 ? null : results[0];
            }

            try
            {
                return await _containerScope.Value
                    .ReadItemAsync<FhirCosmosResourceWrapper>(key.Id, new PartitionKey(key.ToPartitionKey()), cancellationToken: cancellationToken);
            }
            catch (CosmosException exception) when (exception.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }
        }

        public async Task HardDeleteAsync(ResourceKey key, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(key, nameof(key));

            try
            {
                _logger.LogDebug("Obliterating {resourceType}/{id}", key.ResourceType, key.Id);

                StoredProcedureExecuteResponse<IList<string>> response = await _retryExceptionPolicyFactory.GetRetryPolicy().ExecuteAsync(
                    async ct => await _hardDelete.Execute(
                        _containerScope.Value.Scripts,
                        key,
                        ct),
                    cancellationToken);

                _logger.LogDebug("Hard-deleted {count} documents, which consumed {RU} RUs. The list of hard-deleted documents: {resources}.", response.Resource.Count, response.RequestCharge, string.Join(", ", response.Resource));
            }
            catch (CosmosException exception)
            {
                if (exception.IsRequestEntityTooLarge())
                {
                    throw;
                }

                _logger.LogError(exception, "Unhandled Document Client Exception");

                throw;
            }
        }

        public async Task UpdateSearchParameterIndicesBatchAsync(IReadOnlyCollection<ResourceWrapper> resources, CancellationToken cancellationToken)
        {
            // TODO: use batch command to update both hash values and search index values for list updateSearchIndices
            // this is a place holder update until we batch update resources
            foreach (var resource in resources)
            {
                await UpdateSearchIndexForResourceAsync(resource, WeakETag.FromVersionId(resource.Version), cancellationToken);
            }
        }

        public async Task<ResourceWrapper> UpdateSearchIndexForResourceAsync(ResourceWrapper resourceWrapper, WeakETag weakETag, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(resourceWrapper, nameof(resourceWrapper));
            EnsureArg.IsNotNull(weakETag, nameof(weakETag));

            var cosmosWrapper = new FhirCosmosResourceWrapper(resourceWrapper);
            UpdateSortIndex(cosmosWrapper);

            try
            {
                _logger.LogDebug("Replacing {resourceType}/{id}, ETag: \"{tag}\"", resourceWrapper.ResourceTypeName, resourceWrapper.ResourceId, weakETag.VersionId);

                FhirCosmosResourceWrapper response = await _retryExceptionPolicyFactory.GetRetryPolicy().ExecuteAsync(
                    async ct => await _replaceSingleResource.Execute(
                        _containerScope.Value.Scripts,
                        cosmosWrapper,
                        weakETag.VersionId,
                        ct),
                    cancellationToken);

                return response;
            }
            catch (CosmosException exception)
            {
                // Check GetSubStatusCode documentation for why we need to get that instead of the status code.
                switch (exception.GetSubStatusCode())
                {
                    case HttpStatusCode.PreconditionFailed:
                        throw new PreconditionFailedException(string.Format(Core.Resources.ResourceVersionConflict, weakETag));

                    case HttpStatusCode.NotFound:
                        throw new ResourceNotFoundException(string.Format(
                            Core.Resources.ResourceNotFoundByIdAndVersion,
                            resourceWrapper.ResourceTypeName,
                            resourceWrapper.ResourceId,
                            weakETag));

                    case HttpStatusCode.ServiceUnavailable:
                        throw new ServiceUnavailableException();
                }

                _logger.LogError(exception, "Unhandled Document Client Exception");
                throw;
            }
        }

        /// <summary>
        /// Executes a query. If <see cref="FeedOptions.MaxItemCount"/> is set, iterates through the pages returned by Cosmos DB until the result set
        /// has at least half that many results. Paging though subsequent pages times out after <see cref="CosmosDataStoreConfiguration.SearchEnumerationTimeoutInSeconds"/>
        /// or after a 429 response from the DB.
        /// </summary>
        /// <typeparam name="T">The result entry type.</typeparam>
        /// <param name="sqlQuerySpec">The query specification.</param>
        /// <param name="feedOptions">The feed options.</param>
        /// <param name="continuationToken">The continuation token from a previous query.</param>
        /// <param name="mustNotExceedMaxItemCount">If set to true, no more than <see cref="FeedOptions.MaxItemCount"/> entries will be returned. Otherwise, up to 2 * MaxItemCount - 1 items could be returned</param>
        /// <param name="searchEnumerationTimeoutOverride">
        ///     If specified, overrides <see cref="CosmosDataStoreConfiguration.SearchEnumerationTimeoutInSeconds"/> </param> as the maximum amount of time to spend enumerating pages from the SDK to get at least <see cref="QueryRequestOptions.MaxItemCount"/> * <see cref="ExecuteDocumentQueryAsyncMinimumFillFactor"/> results.
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The results and possible continuation token</returns>
        internal async Task<(IReadOnlyList<T> results, string continuationToken)> ExecuteDocumentQueryAsync<T>(QueryDefinition sqlQuerySpec, QueryRequestOptions feedOptions, string continuationToken = null, bool mustNotExceedMaxItemCount = true, TimeSpan? searchEnumerationTimeoutOverride = default, CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(sqlQuerySpec, nameof(sqlQuerySpec));

            var context = new CosmosQueryContext(sqlQuerySpec, feedOptions, continuationToken);
            ICosmosQuery<T> cosmosQuery = null;
            var startTime = Clock.UtcNow;

            FeedResponse<T> page = await _retryExceptionPolicyFactory.GetRetryPolicy().ExecuteAsync(() =>
            {
                cosmosQuery = _cosmosQueryFactory.Create<T>(_containerScope.Value, context); // SDK throws if we don't recreate this on retry
                return cosmosQuery.ExecuteNextAsync(cancellationToken);
            });

            if (!cosmosQuery.HasMoreResults || !feedOptions.MaxItemCount.HasValue || page.Count == feedOptions.MaxItemCount)
            {
                if (page.Count == 0)
                {
                    return (Array.Empty<T>(), page.ContinuationToken);
                }

                var singlePageResults = new List<T>(page.Count);
                singlePageResults.AddRange(page);
                return (singlePageResults, page.ContinuationToken);
            }

            int totalDesiredCount = feedOptions.MaxItemCount.Value;

            // try to obtain at least half of the requested results

            var results = new List<T>(totalDesiredCount);
            results.AddRange(page);

            TimeSpan timeout = (searchEnumerationTimeoutOverride ?? TimeSpan.FromSeconds(_cosmosDataStoreConfiguration.SearchEnumerationTimeoutInSeconds)) - (Clock.UtcNow - startTime);
            if (timeout <= TimeSpan.Zero)
            {
                return (results, page.ContinuationToken);
            }

            using var timeoutTokenSource = new CancellationTokenSource(timeout);
            using var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutTokenSource.Token);

            bool executingWithParallelism = feedOptions.MaxConcurrency != null && continuationToken == null;

            var maxCount = executingWithParallelism
                ? totalDesiredCount * (mustNotExceedMaxItemCount ? 1 : ExecuteDocumentQueryAsyncMaximumFillFactor) // in this mode, the SDK likely has already fetched pages, so we might as well consume them
                : totalDesiredCount * ExecuteDocumentQueryAsyncMinimumFillFactor;

            while (cosmosQuery.HasMoreResults &&
                   (results.Count < maxCount)) // we still want to get more results
            {
                // The FHIR spec says we cannot return more items in a bundle than the _count parameter, if specified.
                // If not specified, mustNotExceedMaxItemCount will be false, and we can allow ourselves to go over the limit.
                // The advantage is that we don't need to construct a new query with a new page size.

                int currentDesiredCount = totalDesiredCount - results.Count;
                if (mustNotExceedMaxItemCount && currentDesiredCount != feedOptions.MaxItemCount && !executingWithParallelism)
                {
                    // Construct a new query with a smaller page size.
                    // We do this to ensure that we will not exceed the original max page size and that
                    // we never have to throw a page of data away because it won't fit in the response.
                    feedOptions.MaxItemCount = currentDesiredCount;
                    context = new CosmosQueryContext(sqlQuerySpec, feedOptions, page.ContinuationToken);
                    cosmosQuery = _cosmosQueryFactory.Create<T>(_containerScope.Value, context);
                }

                try
                {
                    page = await cosmosQuery.ExecuteNextAsync(linkedTokenSource.Token);
                    if (page.Count > 0)
                    {
                        results.AddRange(page);
                        if (mustNotExceedMaxItemCount && results.Count > feedOptions.MaxItemCount)
                        {
                            // we might get here if executingWithParallelism == true

                            int toRemove = results.Count - feedOptions.MaxItemCount.Value;
                            results.RemoveRange(results.Count - toRemove, toRemove);
                            break;
                        }
                    }
                }
                catch (CosmosException e) when (e.IsRequestRateExceeded())
                {
                    // return whatever we have when we get a 429
                    break;
                }
                catch (OperationCanceledException) when (timeoutTokenSource.IsCancellationRequested)
                {
                    // This took too long. Give up.
                    break;
                }
            }

            return (results, page.ContinuationToken);
        }

        private void UpdateSortIndex(FhirCosmosResourceWrapper cosmosWrapper)
        {
            IEnumerable<SearchParameterInfo> searchParameters = _supportedSearchParameters.Value
                .GetSearchParameters(cosmosWrapper.ResourceTypeName)
                .Where(x => x.SortStatus != SortParameterStatus.Disabled);

            if (searchParameters.Any())
            {
                foreach (SearchParameterInfo field in searchParameters)
                {
                    if (cosmosWrapper.SortValues.All(x => x.Value.SearchParameterUri != field.Url))
                    {
                        // Ensure sort property exists
                        cosmosWrapper.SortValues.Add(field.Code, new SortValue(field.Url));
                    }
                }
            }
            else
            {
                cosmosWrapper.SortValues?.Clear();
            }
        }

        public void Build(ICapabilityStatementBuilder builder)
        {
            EnsureArg.IsNotNull(builder, nameof(builder));

            builder.PopulateDefaultResourceInteractions()
                .SyncSearchParameters()
                .AddGlobalSearchParameters()
                .SyncProfiles();

            if (_coreFeatures.SupportsBatch)
            {
                builder.AddGlobalInteraction(SystemRestfulInteraction.Batch);
            }
        }

        public async Task<int?> GetProvisionedDataStoreCapacityAsync(CancellationToken cancellationToken = default)
        {
            return await _containerScope.Value.ReadThroughputAsync(cancellationToken);
        }
    }
}
