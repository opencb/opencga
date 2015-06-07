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
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


@Path("/{version}/projects")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Projects", position = 2, description = "Methods for working with 'projects' endpoint")
public class ProjectWSServer extends OpenCGAWSServer {


    public ProjectWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                           @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @ApiOperation(value = "Create project")
    public Response createProject(@ApiParam(value = "userId", required = true) @QueryParam("userId") String userId,
                                  @ApiParam(value = "name", required = true) @QueryParam("name") String name,
                                  @ApiParam(value = "alias", required = true) @QueryParam("alias") String alias,
                                  @ApiParam(value = "description", required = true) @QueryParam("description") String description,
                                  @ApiParam(value = "organization", required = true) @QueryParam("organization") String organization) {
        try {
            QueryResult queryResult = catalogManager.createProject(userId, name, alias, description, organization, this.getQueryOptions(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }

    }

    @GET
    @Path("/{projectId}/info")
    @ApiOperation(value = "Project information")
    public Response info(@ApiParam(value = "projectId", required = true) @PathParam("projectId") String projectIdsStr) {
        List<QueryResult<Project>> queryResults = new LinkedList<>();
        for (String projectIdStr : projectIdsStr.split(",")) {
            try {
                int projectId = catalogManager.getProjectId(projectIdStr);
                queryResults.add(catalogManager.getProject(projectId, this.getQueryOptions(), sessionId));
            } catch (CatalogException e) {
                e.printStackTrace();
                return createErrorResponse(e.getMessage());
            }
        }
        return createOkResponse(queryResults);
    }

    @GET
    @Path("/{projectId}/studies")
    @ApiOperation(value = "Get all studies the from a project")
    public Response getAllStudies(@ApiParam(value = "projectId", required = true) @PathParam("projectId") String projectIdsStr) {
        String[] splitedId = projectIdsStr.split(",");
        try {
            List<QueryResult> results = new LinkedList<>();
            for (String id : splitedId) {
                results.add(catalogManager.getAllStudies(Integer.parseInt(id), this.getQueryOptions(), sessionId));
            }
            return createOkResponse(results);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{projectId}/update")
    @ApiOperation(value = "Project modify")
    public Response update(@ApiParam(value = "projectId", required = true) @PathParam("projectId") String projectIdStr,
                           @ApiParam(value = "name", required = false) @QueryParam("name") String name,
                           @ApiParam(value = "description", required = false) @QueryParam("description") String description,
                           @ApiParam(value = "organization", required = false) @QueryParam("organization") String organization,
                           @ApiParam(value = "status", required = false) @QueryParam("status") String status,
                           @ApiParam(value = "attributes", required = false) @QueryParam("attributes") String attributes) throws IOException {
        try {
            ObjectMap objectMap = new ObjectMap();
            objectMap.put("name", name);
            objectMap.put("description", description);
            objectMap.put("organization", organization);
            objectMap.put("status", status);
            objectMap.put("attributes", attributes);

            int projectId = catalogManager.getProjectId(projectIdStr);
            QueryResult result = catalogManager.modifyProject(projectId, objectMap, sessionId);
            return createOkResponse(result);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @POST
    @Path("/{projectId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update by POST [NO TESTED]")
    public Response updateByPost(@ApiParam(value = "projectId", required = true) @PathParam("projectId") String projectIdStr,
                                 @ApiParam(value = "params", required = true) Map<String, Object> params) throws IOException {
        try {
            ObjectMap objectMap = new ObjectMap(params);
            int projectId = catalogManager.getProjectId(projectIdStr);
            QueryResult result = catalogManager.modifyProject(projectId, objectMap, sessionId);
            return createOkResponse(result);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{projectId}/delete")
    @ApiOperation(value = "Delete a project [PENDING]")
    public Response delete(@ApiParam(value = "projectId", required = true) @PathParam("projectId") String projectId) {
        return createOkResponse("PENDING");
    }

}