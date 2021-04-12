// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Globalization;
using System.Threading.Tasks;
using Hl7.Fhir.Model;
using MediatR;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Health.Api.Features.Audit;
using Microsoft.Health.Fhir.Api.Features.ActionResults;
using Microsoft.Health.Fhir.Api.Features.Filters;
using Microsoft.Health.Fhir.Api.Features.Routing;
using Microsoft.Health.Fhir.Core.Exceptions;
using Microsoft.Health.Fhir.Core.Extensions;
using Microsoft.Health.Fhir.Core.Features;
using Microsoft.Health.Fhir.Core.Models;
using Microsoft.Health.Fhir.ValueSets;

namespace Microsoft.Health.Fhir.Api.Controllers
{
    [ServiceFilter(typeof(AuditLoggingFilterAttribute))]
    [ServiceFilter(typeof(OperationOutcomeExceptionFilterAttribute))]
    [ServiceFilter(typeof(ValidateFormatParametersAttribute))]
    [ValidateResourceTypeFilter]
    [ValidateModelState]
    public class OperationController : Controller
    {
        private readonly IMediator _mediator;

        public OperationController(IMediator mediator)
        {
            _mediator = mediator;
        }

        [HttpGet]
        [Route(KnownRoutes.EverythingResourceTypeById, Name = RouteNames.EverythingOperationById)]
        [AuditEventType(AuditEventSubType.Everything)]
        public async Task<IActionResult> EverythingById(
            string typeParameter,
            string idParameter,
            [FromQuery(Name = KnownQueryParameterNames.Start)] PartialDateTime start,
            [FromQuery(Name = KnownQueryParameterNames.End)] PartialDateTime end,
            [FromQuery(Name = KnownQueryParameterNames.Since)] PartialDateTime since,
            [FromQuery(Name = KnownQueryParameterNames.Type)] string type,
            [FromQuery(Name = KnownQueryParameterNames.Count)] int? count,
            string ct)
        {
            // $everything operation is currently supported only for Patient resource type.
            if (!string.Equals(typeParameter, ResourceType.Patient.ToString(), StringComparison.Ordinal))
            {
                throw new RequestNotValidException(string.Format(CultureInfo.InvariantCulture, Resources.UnsupportedEverythingOperationResourceType, typeParameter));
            }

            ResourceElement result = await _mediator.GetEverythingResultAsync(typeParameter, idParameter, start, end, since, type, count, ct, HttpContext.RequestAborted);

            return FhirResult.Create(result);
        }
    }
}
