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

package org.opencb.opencga.server.rest;

import io.swagger.annotations.*;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.opencga.analysis.ToolManager;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.models.Tool;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 30/10/14.
 */
@Path("/{version}/tools")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Tools", position = 11, description = "Methods for working with 'tools' endpoint")
public class ToolWSServer extends OpenCGAWSServer {


    public ToolWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(uriInfo, httpServletRequest);
    }

    @GET
    @Path("/{toolId}/info")
    @ApiOperation(value = "Tool info", position = 2, response = Tool.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response info(@PathParam(value = "toolId") @DefaultValue("") @FormDataParam("toolId") String toolId,
                         @ApiParam(value = "execution", required = false)  @DefaultValue("") @QueryParam("execution") String execution) {
        String[] toolIds = toolId.split(",");
        try {
            List<QueryResult> results = new LinkedList<>();
            for (String id : toolIds) {
                QueryResult<Tool> toolResult = catalogManager.getTool(catalogManager.getToolId(id), sessionId);
                Tool tool = toolResult.getResult().get(0);
                ToolManager toolManager = new ToolManager(Paths.get(tool.getPath()).getParent(), tool.getName(), execution);
                tool.setManifest(toolManager.getManifest());
                tool.setResult(toolManager.getResult());
                results.add(toolResult);
            }
            return createOkResponse(results);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search tools", position = 2, response = Tool[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response search(@ApiParam(value = "id", required = false) @QueryParam(value = "id") @DefaultValue("") String toolId,
                           @ApiParam(value = "userId", required = false) @QueryParam(value = "userId") @DefaultValue("") String userId,
                           @ApiParam(value = "alias", required = false) @QueryParam(value = "alias") @DefaultValue("") String alias) {
        try {
            QueryOptions qOptions = new QueryOptions();
            parseQueryParams(params, UserDBAdaptor.ToolQueryParams::getParam, query, qOptions);
            QueryResult<Tool> toolResult = catalogManager.getAllTools(query, qOptions, sessionId);
            for (Tool tool : toolResult.getResult()) {
                ToolManager toolManager = new ToolManager(Paths.get(tool.getPath()).getParent(), tool.getName(), "");
                tool.setManifest(toolManager.getManifest());
                tool.setResult(toolManager.getResult());
            }
            return createOkResponse(toolResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{toolId}/help")
    @ApiOperation(value = "Tool help", position = 3)
    public Response help(@PathParam(value = "toolId") @DefaultValue("") @FormDataParam("toolId") String toolId,
                         @ApiParam(value = "execution", required = false)  @DefaultValue("") @QueryParam("execution") String execution) {
        String[] toolIds = toolId.split(",");
        try {
            List<String> results = new LinkedList<>();
            for (String id : toolIds) {
                Tool tool = catalogManager.getTool(catalogManager.getToolId(id), sessionId).getResult().get(0);
                ToolManager toolManager = new ToolManager(Paths.get(tool.getPath()).getParent(), tool.getName(), execution);
                String help = toolManager.help("");
                System.out.println(help);
                results.add(help);
            }
            return createOkResponse(results);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{toolId}/update")
    @ApiOperation(value = "Update some user attributes using GET method", position = 4, response = Tool.class)
    public Response update(@ApiParam(value = "toolId", required = true) @PathParam("toolId") String toolId) throws IOException {
        return createErrorResponse("update - GET", "PENDING");
    }

    @POST
    @Path("/{toolId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some user attributes using POST method", position = 4)
    public Response updateByPost(@ApiParam(value = "toolId", required = true) @PathParam("toolId") String toolId,
                                 @ApiParam(value = "params", required = true) Map<String, Object> params) {
        return createErrorResponse("update - POST", "PENDING");
    }

    @GET
    @Path("/{toolId}/delete")
    @ApiOperation(value = "Delete a tool", position = 5)
    public Response delete(@ApiParam(value = "toolId", required = true) @PathParam("toolId") String toolId) {
        return createErrorResponse("delete", "PENDING");
    }

}
