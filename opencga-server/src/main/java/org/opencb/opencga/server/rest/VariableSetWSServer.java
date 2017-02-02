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
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.models.Variable;
import org.opencb.opencga.catalog.models.VariableSet;
import org.opencb.opencga.catalog.models.summaries.VariableSetSummary;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.List;

/**
 * Created by jacobo on 16/12/14.
 */
@Path("/{version}/variableset")
@Produces("application/json")
@Api(value = "VariableSet", position = 8, description = "Methods for working with 'variableset' endpoint")
public class VariableSetWSServer extends OpenCGAWSServer {


    public VariableSetWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest);
    }

    private static class VariableSetParameters {
        public Boolean unique;
        public String name;
        public String description;
        public List<Variable> variables;
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create variable set", position = 1, response = VariableSet.class)
    public Response createSet(
            @ApiParam(value = "DEPRECATED: studyId", hidden = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value="JSON containing the variableSet information", required = true) VariableSetParameters params) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            logger.info("variables: {}", params.variables);
            long studyId = catalogManager.getStudyId(studyStr, sessionId);
            QueryResult<VariableSet> queryResult = catalogManager.createVariableSet(studyId, params.name, params.unique, params.description,
                    null, params.variables, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{variablesetId}/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get VariableSet info", position = 2, response = VariableSet.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response variablesetInfo(@ApiParam(value = "variablesetId", required = true) @PathParam("variablesetId") long variablesetId) {
        try {
            // TODO: read param VariableSetParams.STUDY_ID.key()
            QueryResult<VariableSet> queryResult = catalogManager.getVariableSet(variablesetId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{variablesetId}/summary")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get VariableSet summary", position = 2, response = VariableSetSummary.class)
    public Response variablesetSummary(@ApiParam(value = "variablesetId", required = true) @PathParam("variablesetId") long variablesetId) {
        try {
            QueryResult<VariableSetSummary> queryResult = catalogManager.getStudyManager().getVariableSetSummary(variablesetId, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get VariableSet info", position = 2, response = VariableSet[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results. [PENDING]", dataType = "boolean", paramType = "query")
    })
    public Response search(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr,
                           @ApiParam(value = "CSV list of variablesetIds", required = false) @QueryParam("id") String id,
                           @ApiParam(value = "name", required = false) @QueryParam("name") String name,
                           @ApiParam(value = "description", required = false) @QueryParam("description") String description,
                           @ApiParam(value = "attributes", required = false) @QueryParam("attributes") String attributes,
                           @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount) {
        try {
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            QueryResult<VariableSet> queryResult = catalogManager.getStudyManager().searchVariableSets(studyStr, query, queryOptions,
                    sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @GET
//    @Path("/{variablesetId}/update")
//    @ApiOperation(value = "Update some variableset attributes using GET method [PENDING]", position = 3)
//    public Response update(@ApiParam(value = "variablesetId", required = true) @PathParam("variablesetId") String variablesetId,
//                           @ApiParam(value = "name", required = true) @QueryParam("name") String name,
//                           @ApiParam(value = "description", required = false) @QueryParam("description") String description)
// throws IOException {
//        return createErrorResponse("update - GET", "PENDING");
//    }

    private static class VariableSetUpdateParameters {
        public String name;
        public String description;
    }

    @POST
    @Path("/{variablesetId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some variableset attributes using POST method [PENDING]", position = 3, response = VariableSet.class)
    public Response updateByPost(
            @ApiParam(value = "variablesetId", required = true) @PathParam("variablesetId") String variablesetId,
            @ApiParam(value="JSON containing the parameters to be updated", required = true) VariableSetUpdateParameters params) {
        return createErrorResponse("update - POST", "PENDING");
    }

    @GET
    @Path("/{variablesetId}/delete")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Delete an unused variable Set", position = 4)
    public Response delete(@ApiParam(value = "variablesetId", required = true) @PathParam("variablesetId") long variablesetId) {
        try {
            QueryResult<VariableSet> queryResult = catalogManager.deleteVariableSet(variablesetId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{variablesetId}/field/add")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Add a new field in a variable set", position = 5)
    public Response addFieldToVariableSet(@ApiParam(value = "variablesetId", required = true)
                                              @PathParam("variablesetId") long variablesetId,
                                          @ApiParam(value = "variable", required = true) Variable variable) {
        try {
            QueryResult<VariableSet> queryResult = catalogManager.addFieldToVariableSet(variablesetId, variable, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{variablesetId}/field/delete")
    @ApiOperation(value = "Delete one field from a variable set", position = 6)
    public Response renameFieldInVariableSet(@ApiParam(value = "variablesetId", required = true)
                                                 @PathParam("variablesetId") long variablesetId,
                                             @ApiParam(value = "name", required = true) @QueryParam("name") String name) {
        try {
            QueryResult<VariableSet> queryResult = catalogManager.removeFieldFromVariableSet(variablesetId, name, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{variablesetId}/field/rename")
    @ApiOperation(value = "Rename the field id of a field in a variable set", position = 7)
    public Response renameFieldInVariableSet(@ApiParam(value = "variablesetId", required = true)
                                                 @PathParam("variablesetId") long variablesetId,
                                             @ApiParam(value = "oldName", required = true) @QueryParam("oldName") String oldName,
                                             @ApiParam(value = "newName", required = true) @QueryParam("newName") String newName) {
        try {
            QueryResult<VariableSet> queryResult = catalogManager.renameFieldFromVariableSet(variablesetId, oldName, newName, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
