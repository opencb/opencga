/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.server.ws;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Variable;
import org.opencb.opencga.catalog.models.VariableSet;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 16/12/14.
 */
@Path("/{version}/variables")
@Produces("application/json")
@Api(value = "Variables", position = 7, description = "Methods for working with 'variables' endpoint")
public class VariableWSServer extends OpenCGAWSServer {


    public VariableWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                            @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest);
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create variable set", position = 1)
    public Response createSet(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                              @ApiParam(value = "name", required = true) @QueryParam("name") String name,
                              @ApiParam(value = "unique", required = false) @QueryParam("unique") Boolean unique,
                              @ApiParam(value = "description", required = false) @QueryParam("description") String description,
                              @ApiParam(value = "variables", required = true) List<Variable> variables) {
        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
            QueryResult<VariableSet> queryResult = catalogManager.createVariableSet(studyId,
                    name, unique, description, null, variables, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{variableSetId}/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get VariableSet info", position = 2)
    public Response variableSetInfo(@ApiParam(value = "variableSetId", required = true) @PathParam("variableSetId") int variableSetId) {
        try {
            QueryResult<VariableSet> queryResult = catalogManager.getVariableSet(variableSetId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{variableSetId}/update")
    @ApiOperation(value = "Update some user variableSet using GET method [PENDING]", position = 3)
    public Response update(@ApiParam(value = "variableSetId", required = true) @PathParam("variableSetId") String variableSetId) throws IOException {
        return createErrorResponse("update - GET", "PENDING");
    }

    @POST
    @Path("/{variableSetId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some variableSet attributes using POST method [PENDING]", position = 3)
    public Response updateByPost(@ApiParam(value = "variableSetId", required = true) @PathParam("variableSetId") String variableSetId,
                                 @ApiParam(value = "params", required = true) Map<String, Object> params) {
        return createErrorResponse("update - POST", "PENDING");
    }

    @GET
    @Path("/{variableSetId}/delete")
    @ApiOperation(value = "Delete an unused variable Set", position = 4)
    public Response delete(@ApiParam(value = "variableSetId", required = true) @PathParam("variableSetId") int variableSetId) {
        try {
            QueryResult<VariableSet> queryResult = catalogManager.deleteVariableSet(variableSetId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }    }

}
