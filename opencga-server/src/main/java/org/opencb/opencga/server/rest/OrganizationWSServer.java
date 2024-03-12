/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.server.rest;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Path("/{apiVersion}/organizations")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Organizations", description = "Methods for working with 'organizations' endpoint")
public class OrganizationWSServer extends OpenCGAWSServer {

    public OrganizationWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    @GET
    @Path("/{organization}/info")
    @ApiOperation(value = "Return the organization information", response = Organization.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
    })
    public Response getInfo(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION, required = true) @PathParam(ParamConstants.ORGANIZATION) String organizationId
    ) {
        try {
            OpenCGAResult<Organization> result = catalogManager.getOrganizationManager().get(organizationId, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{organization}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some organization attributes", response = Organization.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, dataType = "string", paramType = "query")
    })
    public Response update(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION, required = true) @PathParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "Action to be performed if the array of admins is being updated.", allowableValues = "ADD,REMOVE", defaultValue = "ADD")
                @QueryParam("adminsAction") ParamUtils.AddRemoveAction adminsAction,
            @ApiParam(value = "JSON containing the params to be updated.", required = true) OrganizationUpdateParams parameters) {
        try {
            if (adminsAction == null) {
                adminsAction = ParamUtils.AddRemoveAction.ADD;
            }
            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(OrganizationDBAdaptor.QueryParams.ADMINS.key(), adminsAction);
            queryOptions.put(Constants.ACTIONS, actionMap);
            OpenCGAResult<Organization> result = catalogManager.getOrganizationManager().update(organizationId, parameters, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new organization", response = Organization.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, dataType = "string", paramType = "query")
    })
    public Response create(
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing the organization to be created.", required = true) OrganizationCreateParams parameters) {
        try {
            OpenCGAResult<Organization> result = catalogManager.getOrganizationManager().create(parameters, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}