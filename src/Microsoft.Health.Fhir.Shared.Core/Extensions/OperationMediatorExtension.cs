// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using MediatR;
using Microsoft.Health.Fhir.Core.Messages.Operation;
using Microsoft.Health.Fhir.Core.Models;

namespace Microsoft.Health.Fhir.Core.Extensions
{
    public static class OperationMediatorExtension
    {
        public static async Task<ResourceElement> GetEverythingResultAsync(
            this IMediator mediator,
            string resourceType,
            string resourceId,
            PartialDateTime start = null,
            PartialDateTime end = null,
            PartialDateTime since = null,
            string type = null,
            int? count = null,
            string continuationToken = null,
            CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(mediator, nameof(mediator));

            EverythingOperationResponse result = await mediator.Send(new EverythingOperationRequest(resourceType, resourceId, start, end, since, type, count, continuationToken), cancellationToken);

            return result.Bundle;
        }
    }
}
