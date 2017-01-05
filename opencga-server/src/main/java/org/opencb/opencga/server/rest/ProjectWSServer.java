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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


@Path("/{version}/projects")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Projects", position = 2, description = "Methods for working with 'projects' endpoint")
public class ProjectWSServer extends OpenCGAWSServer {

    public ProjectWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @ApiOperation(value = "Create project", position = 1, response = Project.class)
    public Response createProject(@ApiParam(value = "name", required = true) @QueryParam("name") String name,
                                  @ApiParam(value = "alias", required = true) @QueryParam("alias") String alias,
                                  @ApiParam(value = "description", required = false) @QueryParam("description") String description,
                                  @ApiParam(value = "organization", required = false) @QueryParam("organization") String organization,
                                  @ApiParam(value = "Organism scientific name", required = false) @QueryParam("organism.scientificName") String scientificName,
                                  @ApiParam(value = "Organism common name", required = false) @QueryParam("organism.commonName") String commonName,
                                  @ApiParam(value = "Organism taxonomy code", required = false) @QueryParam("organism.taxonomyCode") String taxonomyCode,
                                  @ApiParam(value = "Organism assembly", required = false) @QueryParam("organism.assembly") String assembly) {
        try {
            QueryResult queryResult = catalogManager.getProjectManager()
                    .create(name, alias, description, organization, scientificName, commonName, taxonomyCode, assembly, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{projects}/info")
    @ApiOperation(value = "Project information", position = 2, response = Project.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response info(@ApiParam(value = "Comma separated list of project ID or alias", required = true) @PathParam("projects") String projectsStr) {
        try {
            String userId = catalogManager.getUserManager().getId(sessionId);
            List<Long> projectIds = catalogManager.getProjectManager().getIds(userId, projectsStr);
            List<QueryResult<Project>> queryResults = new ArrayList<>(projectIds.size());
            for (Long projectId : projectIds) {
                queryResults.add(catalogManager.getProject(projectId, queryOptions, sessionId));
            }
            return createOkResponse(queryResults);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{projects}/studies")
    @ApiOperation(value = "Get all studies the from a project", position = 3, response = Study[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query")
    })
    public Response getAllStudies(@ApiParam(value = "Comma separated list of project ID or alias", required = true) @PathParam("projects") String projectsStr) {
        try {
            String userId = catalogManager.getUserManager().getId(sessionId);
            List<Long> projectIds = catalogManager.getProjectManager().getIds(userId, projectsStr);
            List<QueryResult<Study>> results = new ArrayList<>(projectIds.size());
            String[] splittedProjectNames = projectsStr.split(",");
            for (int i = 0; i < projectIds.size(); i++) {
                Long projectId = projectIds.get(i);
                QueryResult<Study> allStudiesInProject = catalogManager.getAllStudiesInProject(projectId, queryOptions, sessionId);
                // We set the id of the queryResult with the project id given by the user
                allStudiesInProject.setId(splittedProjectNames[i]);
                results.add(allStudiesInProject);
            }
            return createOkResponse(results);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{project}/update")
    @ApiOperation(value = "Project update", position = 4)
    public Response update(@ApiParam(value = "Project ID or alias", required = true) @PathParam("project") String projectStr,
                           @ApiParam(value = "name", required = false) @QueryParam("name") String name,
                           @ApiParam(value = "alias", required = false) @QueryParam("alias") String alias,
                           @ApiParam(value = "description", required = false) @QueryParam("description") String description,
                           @ApiParam(value = "organization", required = false) @QueryParam("organization") String organization,
                           @ApiParam(value = "attributes", required = false) @QueryParam("attributes") String attributes,
                           @ApiParam(value = "Organism scientific name", required = false) @QueryParam("organism.scientificName") String scientificName,
                           @ApiParam(value = "Organism common name", required = false) @QueryParam("organism.commonName") String commonName,
                           @ApiParam(value = "Organism taxonomy code", required = false) @QueryParam("organism.taxonomyCode") String taxonomyCode,
                           @ApiParam(value = "Organism assembly", required = false) @QueryParam("organism.assembly") String assembly) throws IOException {
        try {
            ObjectMap params = new ObjectMap();
            params.putIfNotNull("name", name);
            params.putIfNotNull("alias", alias);
            params.putIfNotNull("description", description);
            params.putIfNotNull("organization", organization);
            params.putIfNotNull("attributes", attributes);
            params.putIfNotNull("organism.scientificName", scientificName);
            params.putIfNotNull("organism.commonName", commonName);
            params.putIfNotNull("organism.taxonomyCode", taxonomyCode);
            params.putIfNotNull("organism.assembly", assembly);

            String userId = catalogManager.getUserManager().getId(sessionId);
            long projectId = catalogManager.getProjectManager().getId(userId, projectStr);
            QueryResult result = catalogManager.modifyProject(projectId, params, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{project}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update by POST [NO TESTED]", position = 4, response = Project.class)
    public Response updateByPost(@ApiParam(value = "Project ID or alias", required = true) @PathParam("project") String projectStr,
                                 @ApiParam(value = "params", required = true) Map<String, Object> params) throws IOException {
        try {
            String userId = catalogManager.getUserManager().getId(sessionId);
            long projectId = catalogManager.getProjectManager().getId(userId, projectStr);

            ObjectMap objectMap = new ObjectMap(params);
            QueryResult result = catalogManager.getProjectManager().update(projectId, objectMap, queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{project}/delete")
    @ApiOperation(value = "Delete a project [PENDING]", position = 5)
    public Response delete(@ApiParam(value = "Project ID or alias", required = true) @PathParam("project") String projectId) {
        return createErrorResponse("delete", "PENDING");
    }

}