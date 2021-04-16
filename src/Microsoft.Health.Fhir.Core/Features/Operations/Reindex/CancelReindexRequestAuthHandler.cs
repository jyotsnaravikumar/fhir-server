// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using MediatR;
using Microsoft.Health.Core.Features.Security.Authorization;
using Microsoft.Health.Fhir.Core.Exceptions;
using Microsoft.Health.Fhir.Core.Features.Security;
using Microsoft.Health.Fhir.Core.Messages.Reindex;

namespace Microsoft.Health.Fhir.Core.Features.Operations.Reindex
{
    public class CancelReindexRequestAuthHandler<TCancelReindexRequest, TCancelReindexResponse> : IPipelineBehavior<CancelReindexRequest, CancelReindexResponse>
    {
        private readonly IAuthorizationService<DataActions> _authorizationService;

        public CancelReindexRequestAuthHandler(IAuthorizationService<DataActions> authorizationService)
        {
            EnsureArg.IsNotNull(authorizationService, nameof(authorizationService));

            _authorizationService = authorizationService;
        }

        public async Task<CancelReindexResponse> Handle(CancelReindexRequest request, CancellationToken cancellationToken, RequestHandlerDelegate<CancelReindexResponse> next)
        {
            if (await _authorizationService.CheckAccess(DataActions.Reindex, cancellationToken) != DataActions.Reindex)
            {
                throw new UnauthorizedFhirActionException();
            }

            return await next();
        }
    }
}
