﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Linq;
using Hl7.Fhir.Model;
using Hl7.Fhir.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Core.Features.Security.Authorization;
using Microsoft.Health.Fhir.Core.Configs;
using Microsoft.Health.Fhir.Core.Features.Operations.ConvertData;
using Microsoft.Health.Fhir.Core.Features.Security;
using Microsoft.Health.Fhir.Core.Messages.ConvertData;
using Microsoft.Health.Fhir.Liquid.Converter.Models;
using Microsoft.Health.Fhir.TemplateManagement.Models;
using Microsoft.Health.Fhir.Tests.Common;
using NSubstitute;
using Xunit;
using Task = System.Threading.Tasks.Task;

namespace Microsoft.Health.Fhir.Shared.Core.UnitTests.Features.Operations.ConvertData
{
    public class ConvertDataRequestHandlerTests
    {
        [Fact]
        public async Task GivenAHl7v2ConvertRequest_WhenConvertData_CorrectResponseShouldReturn()
        {
            var convertDataRequestHandler = GetRequestHandler();
            var response = await convertDataRequestHandler.Handle(GetSampleHl7v2Request(), default);

            var setting = new ParserSettings()
            {
                AcceptUnknownMembers = true,
                PermissiveParsing = true,
            };
            var parser = new FhirJsonParser(setting);
            var bundleResource = parser.Parse<Bundle>(response.Resource);

            var patient = bundleResource.Entry.ByResourceType<Patient>().First();
            Assert.NotEmpty(patient.Id);
            Assert.Equal("DUCK", patient.Name.First().Family);
            Assert.Equal("1924-10-10", patient.BirthDate);
        }

        [Fact]
        public async Task GivenACcdaConvertRequest_WhenConvertData_CorrectResponseShouldReturn()
        {
            var convertDataRequestHandler = GetRequestHandler();
            var response = await convertDataRequestHandler.Handle(GetSampleCcdaRequest(), default);

            var setting = new ParserSettings()
            {
                AcceptUnknownMembers = true,
                PermissiveParsing = true,
            };
            var parser = new FhirJsonParser(setting);
            var bundleResource = parser.Parse<Bundle>(response.Resource);

            var patient = bundleResource.Entry.ByResourceType<Patient>().First();
            Assert.NotEmpty(patient.Id);
            Assert.Equal("Nelson", patient.Name.First().Family);
            Assert.Equal("1962-08-28", patient.BirthDate);
        }

        private ConvertDataRequestHandler GetRequestHandler()
        {
            var convertDataConfig = new ConvertDataConfiguration
            {
                Enabled = true,
                OperationTimeout = TimeSpan.FromSeconds(1),
            };
            convertDataConfig.ContainerRegistryServers.Add("test.azurecr.io");

            IOptions<ConvertDataConfiguration> convertDataConfiguration = Options.Create(convertDataConfig);

            IContainerRegistryTokenProvider tokenProvider = Substitute.For<IContainerRegistryTokenProvider>();
            tokenProvider.GetTokenAsync(Arg.Any<string>(), default).ReturnsForAnyArgs(string.Empty);

            ContainerRegistryTemplateProvider templateProvider = new ContainerRegistryTemplateProvider(tokenProvider, convertDataConfiguration, new NullLogger<ContainerRegistryTemplateProvider>());

            var convertDataEngine = new ConvertDataEngine(
                templateProvider,
                convertDataConfiguration,
                new NullLogger<ConvertDataEngine>());

            IAuthorizationService<DataActions> authorizationService = Substitute.For<IAuthorizationService<DataActions>>();
            authorizationService.CheckAccess(default, default).ReturnsForAnyArgs(DataActions.ConvertData);

            return new ConvertDataRequestHandler(
                authorizationService,
                convertDataEngine,
                convertDataConfiguration);
        }

        private static ConvertDataRequest GetSampleHl7v2Request()
        {
            return new ConvertDataRequest(Samples.SampleHl7v2Message, DataType.Hl7v2, "microsofthealth", true, GetDefaultTemplateImageReferenceByDataType(Liquid.Converter.Models.DataType.Hl7v2), "ADT_A01");
        }

        private static ConvertDataRequest GetSampleCcdaRequest()
        {
            return new ConvertDataRequest(Samples.SampleCcdaMessage, DataType.Ccda, "microsofthealth", true, GetDefaultTemplateImageReferenceByDataType(Liquid.Converter.Models.DataType.Ccda), "CCD");
        }

        private static string GetDefaultTemplateImageReferenceByDataType(Liquid.Converter.Models.DataType dataType)
        {
            return DefaultTemplateInfo.DefaultTemplateMap.Values.FirstOrDefault(value => value.DataType == dataType).ImageReference;
        }
    }
}
