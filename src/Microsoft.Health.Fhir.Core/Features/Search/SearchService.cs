// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Health.Core;
using Microsoft.Health.Fhir.Core.Exceptions;
using Microsoft.Health.Fhir.Core.Features.Persistence;
using Microsoft.Health.Fhir.Core.Models;

namespace Microsoft.Health.Fhir.Core.Features.Search
{
    /// <summary>
    /// Provides the base implementation of the <see cref="ISearchService"/>.
    /// </summary>
    public abstract class SearchService : ISearchService
    {
        private readonly IFhirDataStore _fhirDataStore;

        /// <summary>
        /// Initializes a new instance of the <see cref="SearchService"/> class.
        /// </summary>
        /// <param name="searchOptionsFactory">The search options factory.</param>
        /// <param name="fhirDataStore">The data store</param>
        /// <param name="modelInfoProvider">The model info provider</param>
        protected SearchService(ISearchOptionsFactory searchOptionsFactory, IFhirDataStore fhirDataStore)
        {
            EnsureArg.IsNotNull(searchOptionsFactory, nameof(searchOptionsFactory));
            EnsureArg.IsNotNull(fhirDataStore, nameof(fhirDataStore));

            SearchOptionsFactory = searchOptionsFactory;
            _fhirDataStore = fhirDataStore;
        }

        protected ISearchOptionsFactory SearchOptionsFactory { get; }

        /// <inheritdoc />
        public async Task<SearchResult> SearchAsync(
            string resourceType,
            IReadOnlyList<Tuple<string, string>> queryParameters,
            CancellationToken cancellationToken)
        {
            SearchOptions searchOptions = SearchOptionsFactory.Create(resourceType, queryParameters);

            // Execute the actual search.
            return await SearchInternalAsync(searchOptions, cancellationToken);
        }

        /// <inheritdoc />
        public async Task<SearchResult> SearchCompartmentAsync(
            string compartmentType,
            string compartmentId,
            string resourceType,
            IReadOnlyList<Tuple<string, string>> queryParameters,
            CancellationToken cancellationToken)
        {
            SearchOptions searchOptions = SearchOptionsFactory.Create(compartmentType, compartmentId, resourceType, queryParameters);

            // Execute the actual search.
            return await SearchInternalAsync(searchOptions, cancellationToken);
        }

        public async Task<SearchResult> SearchHistoryAsync(
            string resourceType,
            string resourceId,
            PartialDateTime at,
            PartialDateTime since,
            PartialDateTime before,
            int? count,
            string continuationToken,
            CancellationToken cancellationToken)
        {
            var queryParameters = new List<Tuple<string, string>>();

            if (at != null)
            {
                if (since != null)
                {
                    // _at and _since cannot be both specified.
                    throw new InvalidSearchOperationException(
                        string.Format(
                            CultureInfo.InvariantCulture,
                            Core.Resources.AtCannotBeSpecifiedWithBeforeOrSince,
                            KnownQueryParameterNames.At,
                            KnownQueryParameterNames.Since));
                }

                if (before != null)
                {
                    // _at and _since cannot be both specified.
                    throw new InvalidSearchOperationException(
                        string.Format(
                            CultureInfo.InvariantCulture,
                            Core.Resources.AtCannotBeSpecifiedWithBeforeOrSince,
                            KnownQueryParameterNames.At,
                            KnownQueryParameterNames.Before));
                }
            }

            if (before != null)
            {
                var beforeOffset = before.ToDateTimeOffset(
                    defaultMonth: 1,
                    defaultDaySelector: (year, month) => 1,
                    defaultHour: 0,
                    defaultMinute: 0,
                    defaultSecond: 0,
                    defaultFraction: 0.0000000m,
                    defaultUtcOffset: TimeSpan.Zero).ToUniversalTime();

                if (beforeOffset.CompareTo(Clock.UtcNow) > 0)
                {
                    // you cannot specify a value for _before in the future
                    throw new InvalidSearchOperationException(
                        string.Format(
                            CultureInfo.InvariantCulture,
                            Core.Resources.HistoryParameterBeforeCannotBeFuture,
                            KnownQueryParameterNames.Before));
                }
            }

            bool searchByResourceId = !string.IsNullOrEmpty(resourceId);

            if (searchByResourceId)
            {
                queryParameters.Add(Tuple.Create(SearchParameterNames.Id, resourceId));
            }

            if (!string.IsNullOrEmpty(continuationToken))
            {
                queryParameters.Add(Tuple.Create(KnownQueryParameterNames.ContinuationToken, continuationToken));
            }

            if (at != null)
            {
                queryParameters.Add(Tuple.Create(KnownQueryParameterNames.At, at.ToString()));
            }
            else
            {
                if (since != null)
                {
                    queryParameters.Add(Tuple.Create(SearchParameterNames.LastUpdated, $"ge{since}"));
                }

                if (before != null)
                {
                    queryParameters.Add(Tuple.Create(SearchParameterNames.LastUpdated, $"lt{before}"));
                }
            }

            if (count.HasValue && count > 0)
            {
                queryParameters.Add(Tuple.Create(KnownQueryParameterNames.Count, count.ToString()));
            }

            SearchOptions searchOptions = SearchOptionsFactory.Create(resourceType, queryParameters);

            SearchResult searchResult = await SearchHistoryInternalAsync(searchOptions, cancellationToken);

            // If no results are returned from the _history search
            // determine if the resource actually exists or if the results were just filtered out.
            // The 'deleted' state has no effect because history will return deleted resources
            if (searchByResourceId && searchResult.Results.Any() == false)
            {
                var resource = await _fhirDataStore.GetAsync(new ResourceKey(resourceType, resourceId), cancellationToken);

                if (resource == null)
                {
                    throw new ResourceNotFoundException(string.Format(Core.Resources.ResourceNotFoundById, resourceType, resourceId));
                }
            }

            return searchResult;
        }

        public async Task<SearchResult> SearchForReindexAsync(
            IReadOnlyList<Tuple<string, string>> queryParameters,
            string searchParameterHash,
            bool countOnly,
            CancellationToken cancellationToken)
        {
            SearchOptions searchOptions = SearchOptionsFactory.Create(null, queryParameters);

            if (countOnly)
            {
                searchOptions.CountOnly = true;
            }

            var results = await SearchForReindexInternalAsync(searchOptions, searchParameterHash, cancellationToken);

            return results;
        }

        public async Task<SearchResult> SearchForEverythingOperationAsync(
            string resourceType,
            string resourceId,
            PartialDateTime start,
            PartialDateTime end,
            PartialDateTime since,
            string type,
            int? count,
            string continuationToken,
            IReadOnlyList<string> includes,
            IReadOnlyList<Tuple<string, string>> revincludes,
            CancellationToken cancellationToken)
        {
            return await SearchForEverythingOperationInternalAsync(resourceType, resourceId, start, end, since, type, count, continuationToken, includes, revincludes, cancellationToken);
        }

        /// <summary>
        /// Performs the actual search.
        /// </summary>
        /// <param name="searchOptions">The options to use during the search.</param>
        /// <param name="cancellationToken">The cancellationToken.</param>
        /// <returns>The search result.</returns>
        protected abstract Task<SearchResult> SearchInternalAsync(
            SearchOptions searchOptions,
            CancellationToken cancellationToken);

        protected abstract Task<SearchResult> SearchHistoryInternalAsync(
            SearchOptions searchOptions,
            CancellationToken cancellationToken);

        protected abstract Task<SearchResult> SearchForReindexInternalAsync(
            SearchOptions searchOptions,
            string searchParameterHash,
            CancellationToken cancellationToken);

        protected abstract Task<SearchResult> SearchForEverythingOperationInternalAsync(
            string resourceType,
            string resourceId,
            PartialDateTime start,
            PartialDateTime end,
            PartialDateTime since,
            string type,
            int? count,
            string continuationToken,
            IReadOnlyList<string> includeParameters,
            IReadOnlyList<Tuple<string, string>> revincludeParameters,
            CancellationToken cancellationToken);

        protected async Task<SearchResult> SearchReferencesForEverythingOperation(
            string resourceType,
            string resourceId,
            PartialDateTime since,
            string type,
            IReadOnlyList<string> includeParameters,
            IReadOnlyList<Tuple<string, string>> revincludeParameters,
            CancellationToken cancellationToken)
        {
            // build search parameters
            var searchParameters = new List<Tuple<string, string>>
            {
                Tuple.Create(SearchParameterNames.Id, resourceId),
            };

            searchParameters.AddRange(includeParameters.Select(include => Tuple.Create(SearchParameterNames.Include, $"{resourceType}:{include}")));
            searchParameters.AddRange(revincludeParameters.Select(revinclude => Tuple.Create(SearchParameterNames.Revinclude, $"{revinclude.Item1}:{revinclude.Item2}")));

            // search
            SearchOptions searchOptions = SearchOptionsFactory.Create(resourceType, searchParameters);
            SearchResult searchResult = await SearchInternalAsync(searchOptions, cancellationToken);
            IEnumerable<SearchResultEntry> searchResultEntries = searchResult.Results.Select(x => new SearchResultEntry(x.Resource));

            // filter results by _type
            if (!string.IsNullOrEmpty(type))
            {
                IReadOnlyList<string> types = type.SplitByOrSeparator();
                searchResultEntries = searchResultEntries.Where(s => types.Contains(s.Resource.ResourceTypeName));
            }

            // filter results by _since
            if (since != null)
            {
                var sinceDateTimeOffset = since.ToDateTimeOffset(
                    defaultMonth: 1,
                    defaultDaySelector: (year, month) => 1,
                    defaultHour: 0,
                    defaultMinute: 0,
                    defaultSecond: 0,
                    defaultFraction: 0.0000000m,
                    defaultUtcOffset: TimeSpan.Zero);
                searchResultEntries = searchResultEntries.Where(s => s.Resource.LastModified.CompareTo(sinceDateTimeOffset) >= 0).ToList();
            }

            return new SearchResult(searchResultEntries, searchResult.ContinuationToken, searchResult.SortOrder, searchResult.UnsupportedSearchParameters);
        }
    }
}
