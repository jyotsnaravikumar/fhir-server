// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Hl7.Fhir.Model;
using MediatR;
using Microsoft.Health.Fhir.Api.Controllers;
using Microsoft.Health.Fhir.Core.Exceptions;
using NSubstitute;
using Xunit;
using Task = System.Threading.Tasks.Task;

namespace Microsoft.Health.Fhir.Api.UnitTests.Controllers
{
    public class OperationControllerTests
    {
        private OperationController _operationController;
        private IMediator _mediator = Substitute.For<IMediator>();

        public OperationControllerTests()
        {
            _operationController = GetController();
        }

        [Fact]
        public async Task GivenAnEverythingOperationRequest_WhenResourceTypeIsNotPatient_ThenRequestNotValidExceptionShouldBeThrown()
        {
            await Assert.ThrowsAsync<RequestNotValidException>(() => _operationController.EverythingById(
                type: ResourceType.Observation.ToString(),
                idParameter: null,
                typeParameter: null,
                start: null,
                end: null,
                since: null,
                count: null,
                ct: null));
        }

        private OperationController GetController()
        {
            return new OperationController(_mediator);
        }
    }
}
