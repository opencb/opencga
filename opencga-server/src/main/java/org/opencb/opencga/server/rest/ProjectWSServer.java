/*
 * Copyright 2015-2017 OpenCB
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
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.server.WebServiceException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;
import java.util.Map;


@Path("/{apiVersion}/projects")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Projects", position = 2, description = "Methods for working with 'projects' endpoint")
public class ProjectWSServer extends OpenCGAWSServer {

    public ProjectWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create a new project", response = Project.class)
    public Response createProjectPOST(@ApiParam(value = "JSON containing the mandatory parameters", required = true) ProjectCreateParams project) {
        try {
            ObjectUtils.defaultIfNull(project, new ProjectCreateParams());
            QueryResult queryResult = catalogManager.getProjectManager()
                    .create(project.name, project.alias, project.description, project.organization,
                            project.organism != null ? project.organism.getScientificName() : null,
                            project.organism != null ? project.organism.getCommonName() : null,
                            project.organism != null ? Integer.toString(project.organism.getTaxonomyCode()) : null,
                            project.organism != null ? project.organism.getAssembly() : null, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{projects}/info")
    @ApiOperation(value = "Fetch project information", response = Project.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Set which fields are included in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Set which fields are excluded in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query")
    })
    public Response info(@ApiParam(value = "Comma separated list of project IDs or aliases up to a maximum of 100", required = true) @PathParam("projects")
                                 String projects,
                         @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false") @QueryParam("silent") boolean silent) {

        try {
            List<String> idList = getIdList(projects);
            return createOkResponse(catalogManager.getProjectManager().get(idList, queryOptions, silent, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search projects", response = Project[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Set which fields are included in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Set which fields are excluded in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Max number of results to be returned.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to be skipped.", dataType = "integer", paramType = "query")
    })
    public Response searchProjects(
            @ApiParam(value = "Owner of the project") @QueryParam("owner") String owner,
            @ApiParam(value = "Project name") @QueryParam("name") String name,
            @ApiParam(value = "Project alias") @QueryParam("alias") String alias,
            @ApiParam(value = "Project organization") @QueryParam("organization") String organization,
            @ApiParam(value = "Project description") @QueryParam("description") String description,
            @ApiParam(value = "Study id or alias") @QueryParam("study") String study,
            @ApiParam(value = "Creation date") @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Attributes") @QueryParam("attributes") String attributes) {
        try {
            if (StringUtils.isNotEmpty(owner)) {
                query.remove("owner");
                query.put(ProjectDBAdaptor.QueryParams.USER_ID.key(), owner);
            }

            QueryResult<Project> queryResult = catalogManager.getProjectManager().get(query, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{project}/increlease")
    @ApiOperation(value = "Increment current release number in the project", response = Integer.class)
    public Response incrementRelease(
            @ApiParam(value = "Project ID or alias", required = true) @PathParam("project") String projectStr) {
        try {
            isSingleId(projectStr);
            return createOkResponse(catalogManager.getProjectManager().incrementRelease(projectStr, sessionId));
        } catch (CatalogException | WebServiceException e) {
            e.printStackTrace();
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{projects}/studies")
    @ApiOperation(value = "Fetch all the studies contained in the projects", response = Study[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Set which fields are included in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Set which fields are excluded in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Max number of results to be returned.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to be skipped.", dataType = "integer", paramType = "query")
    })
    public Response getAllStudies(@ApiParam(value = "Comma separated list of project ID or alias up to a maximum of 100", required = true)
                                  @PathParam("projects") String projects,
                                  @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            List<String> idList = getIdList(projects);
            return createOkResponse(catalogManager.getStudyManager().get(idList, new Query(), queryOptions, silent, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{project}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some project attributes", response = Project.class)
    public Response updateByPost(@ApiParam(value = "Project id or alias", required = true) @PathParam("project") String projectStr,
                                 @ApiParam(value = "JSON containing the params to be updated. It will be only possible to update organism "
                                         + "fields not previously defined.", required = true) ProjectUpdateParams updateParams)
            throws IOException {
        try {
            ObjectUtils.defaultIfNull(updateParams, new ProjectUpdateParams());

            isSingleId(projectStr);
            ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(updateParams));
            if (updateParams.organism != null) {
                if (StringUtils.isNotEmpty(updateParams.organism.getAssembly())) {
                    params.append(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key(), updateParams.organism.getAssembly());
                }
                if (StringUtils.isNotEmpty(updateParams.organism.getCommonName())) {
                    params.append(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key(), updateParams.organism.getCommonName());
                }
                if (StringUtils.isNotEmpty(updateParams.organism.getScientificName())) {
                    params.append(ProjectDBAdaptor.QueryParams.ORGANISM_SCIENTIFIC_NAME.key(), updateParams.organism.getScientificName());
                }
                if (updateParams.organism.getTaxonomyCode() > 0) {
                    params.append(ProjectDBAdaptor.QueryParams.ORGANISM_TAXONOMY_CODE.key(), updateParams.organism.getTaxonomyCode());
                }
                params.remove("organism");
            }

            QueryResult result = catalogManager.getProjectManager().update(projectStr, params, queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{project}/delete")
    @ApiOperation(value = "Delete a project [WARNING]", position = 5,
            notes = "Usage of this webservice might lead to unexpected behaviour and therefore is discouraged to use. Deletes are " +
                    "planned to be fully implemented and tested in version 1.4.0")
    public Response delete(@ApiParam(value = "Project ID or alias", required = true) @PathParam("project") String projectId) {
        return createErrorResponse("delete", "PENDING");
    }

    protected static class ProjectParams {
        public String name;
        public String description;
        public String organization;
        public Project.Organism organism;
    }

    protected static class ProjectCreateParams extends ProjectParams {
        public String alias;
    }

    protected static class ProjectUpdateParams extends ProjectParams {
        public Map<String, Object> attributes;
    }

}