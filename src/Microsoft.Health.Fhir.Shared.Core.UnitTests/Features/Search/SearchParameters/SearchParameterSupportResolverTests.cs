﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Core.Features.Search.Parameters;
using Microsoft.Health.Fhir.Core.Models;
using Microsoft.Health.Fhir.ValueSets;
using Xunit;

namespace Microsoft.Health.Fhir.Core.UnitTests.Features.Search
{
    public class SearchParameterSupportResolverTests : IClassFixture<SearchParameterFixtureData>, IAsyncLifetime
    {
        private readonly SearchParameterFixtureData _fixture;
        private SearchParameterSupportResolver _resolver;

        public SearchParameterSupportResolverTests(SearchParameterFixtureData fixture) => _fixture = fixture;

        public async Task InitializeAsync()
        {
            _resolver = new SearchParameterSupportResolver(
                await _fixture.GetSearchDefinitionManagerAsync(),
                await SearchParameterFixtureData.GetFhirTypedElementToSearchValueConverterManagerAsync());
        }

        public Task DisposeAsync() => Task.CompletedTask;

        [Fact]
        public void GivenASupportedSearchParameter_WhenResolvingSupport_ThenTrueIsReturned()
        {
            var sp = new SearchParameterInfo(
                "Condition-abatement-age",
                "Condition-abatement-age",
                SearchParamType.Quantity,
                new Uri("http://hl7.org/fhir/SearchParameter/Condition-abatement-age"),
                expression: "Condition.abatement.as(Range)",
                baseResourceTypes: new[] { "Condition" });

            var supported = _resolver.IsSearchParameterSupported(sp);

            Assert.True(supported.Supported);
            Assert.False(supported.IsPartiallySupported);
        }

        [Fact]
        public void GivenAnUnsupportedSearchParameter_WhenResolvingSupport_ThenFalseIsReturned()
        {
            var sp = new SearchParameterInfo(
                "Condition-abatement-age",
                "Condition-abatement-age",
                SearchParamType.Uri,
                new Uri("http://hl7.org/fhir/SearchParameter/Condition-abatement-age"),
                expression: "Condition.abatement.as(Range)",
                baseResourceTypes: new[] { "Condition" });

            // Can not convert Range => Uri
            var supported = _resolver.IsSearchParameterSupported(sp);

            Assert.False(supported.Supported);
            Assert.False(supported.IsPartiallySupported);
        }

        [Fact]
        public void GivenAPartiallySupportedSearchParameter_WhenResolvingSupport_ThenTrueIsReturned()
        {
            var sp = new SearchParameterInfo(
                "Condition-abatement-age",
                "Condition-abatement-age",
                SearchParamType.Quantity,
                new Uri("http://hl7.org/fhir/SearchParameter/Condition-abatement-age"),
                expression: "Condition.asserter | Condition.abatement.as(Range)",
                baseResourceTypes: new[] { "Condition" });

            // Condition.asserter cannot be translated to Quantity
            // Condition.abatement.as(Range) CAN be translated to Quantity
            var supported = _resolver.IsSearchParameterSupported(sp);

            Assert.True(supported.Supported);
            Assert.True(supported.IsPartiallySupported);
        }

        [Fact]
        public void GivenASearchParameterWithBadType_WhenResolvingSupport_ThenFalseIsReturned()
        {
            var sp = new SearchParameterInfo(
                "Condition-abatement-age",
                "Condition-abatement-age",
                SearchParamType.Quantity,
                new Uri("http://hl7.org/fhir/SearchParameter/Condition-abatement-age"),
                expression: "Condition.asserter | Condition.abatement.as(Range)",
                baseResourceTypes: new[] { "UnknownType" });

            var supported = _resolver.IsSearchParameterSupported(sp);

            Assert.False(supported.Supported);
            Assert.False(supported.IsPartiallySupported);
        }

        [Fact]
        public void GivenASearchParameterWithNoBaseTypes_WhenResolvingSupport_ThenAnExceptionIsThrown()
        {
            var sp = new SearchParameterInfo(
                "Condition-abatement-age",
                "Condition-abatement-age",
                SearchParamType.Quantity,
                new Uri("http://hl7.org/fhir/SearchParameter/Condition-abatement-age"),
                expression: "Condition.asserter | Condition.abatement.as(Range)",
                baseResourceTypes: new string[0]);

            Assert.Throws<NotSupportedException>(() => _resolver.IsSearchParameterSupported(sp));
        }
    }
}
