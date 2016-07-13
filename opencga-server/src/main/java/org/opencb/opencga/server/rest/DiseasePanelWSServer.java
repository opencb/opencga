package org.opencb.opencga.server.rest;

import io.swagger.annotations.*;
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
@Api(value = "Panels", position = 10, description = "Methods for working with 'panels' endpoint")
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
    @Path("/{panelId}/acls")
    @ApiOperation(value = "Returns the acls of the panel [PENDING]", position = 18)
    public Response getAcls(@ApiParam(value = "panelId", required = true) @PathParam("panelId") String studyIdStr) {
        try {
            return createOkResponse(null);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{panelId}/acls/create")
    @ApiOperation(value = "Define a set of permissions for a list of members [PENDING]", position = 19)
    public Response createRole(@ApiParam(value = "panelId", required = true) @PathParam("panelId") String studyIdStr,
                               @ApiParam(value = "Template of permissions to be used (admin, analyst or locked)", required = false) @DefaultValue("") @QueryParam("templateId") String roleId,
                               @ApiParam(value = "Comma separated list of permissions that will be granted to the member list", required = true) @DefaultValue("") @QueryParam("permissions") String permissions,
                               @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members) {
        try {
            return createOkResponse(null);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{panelId}/acls/{memberId}/info")
    @ApiOperation(value = "Returns the set of permissions granted for the member [PENDING]", position = 20)
    public Response getAcl(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                           @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(null);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{panelId}/acls/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member [PENDING]", position = 21)
    public Response updateAcl(@ApiParam(value = "panelId", required = true) @PathParam("panelId") String studyIdStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
                              @ApiParam(value = "Comma separated list of permissions to add", required = false) @PathParam("addPermissions") String addPermissions,
                              @ApiParam(value = "Comma separated list of permissions to remove", required = false) @PathParam("removePermissions") String removePermissions,
                              @ApiParam(value = "Comma separated list of permissions to set", required = false) @PathParam("setPermissions") String setPermissions) {
        try {
            return createOkResponse(null);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{panelId}/acls/{memberId}/delete")
    @ApiOperation(value = "Delete all the permissions granted for the member [PENDING]", position = 22)
    public Response deleteAcl(@ApiParam(value = "panelId", required = true) @PathParam("panelId") String studyIdStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(null);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


}
