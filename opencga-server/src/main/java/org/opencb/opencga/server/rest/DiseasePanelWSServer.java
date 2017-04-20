/*
 * Copyright 2015-2016 OpenCB
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

import io.swagger.annotations.*;
import org.apache.commons.lang.NotImplementedException;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.DiseasePanel;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by pfurio on 01/06/16.
 */

@Path("/{version}/panels")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Panels", hidden = true, position = 10, description = "Methods for working with 'panels' endpoint")
public class DiseasePanelWSServer extends OpenCGAWSServer {

    public DiseasePanelWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @ApiOperation(value = "Create disease panel", position = 1, response = DiseasePanel.class)
    public Response createSample(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                 @ApiParam(value = "name", required = true) @QueryParam("name") String name,
                                 @ApiParam(value = "disease", required = true) @QueryParam("disease") String disease,
                                 @ApiParam(value = "description", required = false) @QueryParam("description") String description,
                                 @ApiParam(value = "genes", required = false) @QueryParam("genes") String genes,
                                 @ApiParam(value = "regions", required = false) @QueryParam("regions") String regions,
                                 @ApiParam(value = "variants", required = false) @QueryParam("variants") String variants) {
        try {
            QueryResult<DiseasePanel> queryResult = catalogManager.createDiseasePanel(studyIdStr, name, disease, description, genes,
                    regions, variants, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{panelId}/info")
    @ApiOperation(value = "Get disease panel information", position = 2, response = DiseasePanel.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response infoSample(@ApiParam(value = "panelId", required = true) @PathParam("panelId") String panelId) {
        try {
            QueryResult<DiseasePanel> queryResult = catalogManager.getDiseasePanel(panelId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{panelIds}/acl")
    @ApiOperation(value = "Return the acl of the panel", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of panel ids", required = true) @PathParam("panelIds") String panelIdsStr) {
//        try {
//            return createOkResponse(catalogManager.getAllPanelAcls(panelIdsStr, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
        return createErrorResponse(new NotImplementedException());
    }


    @GET
    @Path("/{panelIds}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of members", hidden = true, position = 19)
    public Response createRole(@ApiParam(value = "Comma separated list of panel ids", required = true) @PathParam("panelIds") String panelIdsStr,
                               @ApiParam(value = "Comma separated list of permissions that will be granted to the member list", required = false) @DefaultValue("") @QueryParam("permissions") String permissions,
                               @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members) {
//        try {
//            return createOkResponse(catalogManager.createPanelAcls(panelIdsStr, members, permissions, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
        return createErrorResponse(new NotImplementedException());
    }

    @GET
    @Path("/{panelId}/acl/{memberId}/info")
    @ApiOperation(value = "Return the set of permissions granted for the member", position = 20)
    public Response getAcl(@ApiParam(value = "Panel id", required = true) @PathParam("panelId") String panelIdStr,
                           @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
//        try {
//            return createOkResponse(catalogManager.getPanelAcl(panelIdStr, memberId, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
        return createErrorResponse(new NotImplementedException());
    }

    @GET
    @Path("/{panelId}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", hidden = true, position = 21)
    public Response updateAcl(@ApiParam(value = "panelId", required = true) @PathParam("panelId") String panelIdStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
                              @ApiParam(value = "Comma separated list of permissions to add", required = false) @QueryParam("add")
                                          String addPermissions,
                              @ApiParam(value = "Comma separated list of permissions to remove", required = false) @QueryParam("remove")
                                          String removePermissions,
                              @ApiParam(value = "Comma separated list of permissions to set", required = false) @QueryParam("set")
                                          String setPermissions) {
//        try {
//            return createOkResponse(catalogManager.updatePanelAcl(panelIdStr, memberId, addPermissions, removePermissions, setPermissions, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
        return createErrorResponse(new NotImplementedException());
    }

    @GET
    @Path("/{panelIds}/acl/{memberId}/delete")
    @ApiOperation(value = "Delete all the permissions granted for the member", position = 22)
    public Response deleteAcl(@ApiParam(value = "Comma separated list of panel ids", required = true) @PathParam("panelIds") String panelIdsStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
//        try {
//            return createOkResponse(catalogManager.removePanelAcl(panelIdsStr, memberId, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
        return createErrorResponse(new NotImplementedException());
    }


}
