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
@Path("/{version}/variables")
@Produces("application/json")
@Api(value = "Variables", description = "Methods for working with 'variables' endpoint")
public class VariableWSServer extends OpenCGAWSServer {


    public VariableWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                            @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
//        params = uriInfo.getQueryParameters();
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create variable set")
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
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{variableSetId}/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get VariableSet info")
    public Response variableSetInfo(@ApiParam(value = "variableSetId", required = true) @PathParam("variableSetId") int variableSetId) {
        try {
            QueryResult<VariableSet> queryResult = catalogManager.getVariableSet(variableSetId, this.getQueryOptions(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

}
