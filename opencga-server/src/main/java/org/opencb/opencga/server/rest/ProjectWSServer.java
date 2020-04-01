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

import io.swagger.annotations.*;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.QueryParam;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;


@Path("/{apiVersion}/projects")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Projects", description = "Methods for working with 'projects' endpoint")
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

            OpenCGAResult<Project> queryResult = catalogManager.getProjectManager()
                    .create(project.getId(), project.getName(), project.getDescription(),
                            project.getOrganism() != null ? project.getOrganism().getScientificName() : null,
                            project.getOrganism() != null ? project.getOrganism().getCommonName() : null,
                            project.getOrganism() != null ? project.getOrganism().getAssembly() : null, queryOptions, token);
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
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response info(
            @ApiParam(value = ParamConstants.PROJECTS_DESCRIPTION, required = true) @PathParam("projects") String projects) {

        try {
            List<String> idList = getIdList(projects);
            return createOkResponse(catalogManager.getProjectManager().get(idList, queryOptions, true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search projects", response = Project.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query")
    })
    public Response searchProjects(
            @ApiParam(value = "Owner of the project") @QueryParam("owner") String owner,
            @ApiParam(value = ParamConstants.PROJECT_DESCRIPTION) @QueryParam("id") String id,
            @ApiParam(value = "Project name") @QueryParam("name") String name,
            @ApiParam(value = "Project fqn") @QueryParam("fqn") String fqn,
            @ApiParam(value = "DEPRECATED: Project alias") @QueryParam("alias") String alias,
            @ApiParam(value = "Project organization") @QueryParam("organization") String organization,
            @ApiParam(value = "Project description") @QueryParam("description") String description,
            @ApiParam(value = "Study id or alias") @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION)
            @QueryParam("creationDate") String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION)
            @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Attributes") @QueryParam("attributes") String attributes) {
        try {
            if (StringUtils.isNotEmpty(owner)) {
                query.remove("owner");
                query.put(ProjectDBAdaptor.QueryParams.USER_ID.key(), owner);
            }

            DataResult<Project> queryResult = catalogManager.getProjectManager().get(query, queryOptions, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{projects}/aggregationStats")
    @ApiOperation(value = "Fetch catalog project stats", response = FacetField.class)
    public Response getAggregationStats(
            @ApiParam(value = ParamConstants.PROJECTS_DESCRIPTION, required = true) @PathParam("projects") String projects,
            @ApiParam(value = "Calculate default stats", defaultValue = "true") @QueryParam("default") Boolean defaultStats,
            @ApiParam(value = "List of file fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("fileFields") String fileFields,
            @ApiParam(value = "List of individual fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("individualFields") String individualFields,
            @ApiParam(value = "List of family fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("familyFields") String familyFields,
            @ApiParam(value = "List of sample fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("sampleFields") String sampleFields,
            @ApiParam(value = "List of cohort fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("cohortFields") String cohortFields,
            @ApiParam(value = "List of job fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("jobFields") String jobFields) {
        try {
            if (defaultStats == null) {
                defaultStats = true;
            }
            List<String> idList = getIdList(projects);
            Map<String, Object> result = new HashMap<>();
            for (String project : idList) {
                result.put(project, catalogManager.getProjectManager().facet(project, fileFields, sampleFields, individualFields,
                        cohortFields, familyFields, jobFields, defaultStats, token));
            }
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{project}/incRelease")
    @ApiOperation(value = "Increment current release number in the project", response = Integer.class)
    public Response incrementRelease(
            @ApiParam(value = ParamConstants.PROJECT_DESCRIPTION, required = true) @PathParam(ParamConstants.PROJECT_PARAM) String projectStr) {
        try {
            ParamUtils.checkIsSingleID(projectStr);
            return createOkResponse(catalogManager.getProjectManager().incrementRelease(projectStr, token));
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{project}/studies")
    @ApiOperation(value = "Fetch all the studies contained in the project", response = Study.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query")
    })
    public Response getAllStudies(
            @ApiParam(value = ParamConstants.PROJECT_DESCRIPTION, required = true) @PathParam(ParamConstants.PROJECT_PARAM) String project) {
        try {
            return createOkResponse(catalogManager.getStudyManager().get(project, new Query(), queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{project}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some project attributes", response = Project.class)
    public Response updateByPost(
            @ApiParam(value = ParamConstants.PROJECT_DESCRIPTION, required = true) @PathParam(ParamConstants.PROJECT_PARAM) String projectStr,
            @ApiParam(value = "JSON containing the params to be updated. It will be only possible to update organism "
                                         + "fields not previously defined.", required = true) ProjectUpdateParams updateParams) {
        try {
            ObjectUtils.defaultIfNull(updateParams, new ProjectUpdateParams());

            ParamUtils.checkIsSingleID(projectStr);
            ObjectMap params = new ObjectMap(getUpdateObjectMapper().writeValueAsString(updateParams));
            if (updateParams.getOrganism() != null) {
                if (StringUtils.isNotEmpty(updateParams.getOrganism().getAssembly())) {
                    params.append(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key(), updateParams.getOrganism().getAssembly());
                }
                if (StringUtils.isNotEmpty(updateParams.getOrganism().getCommonName())) {
                    params.append(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key(), updateParams.getOrganism().getCommonName());
                }
                if (StringUtils.isNotEmpty(updateParams.getOrganism().getScientificName())) {
                    params.append(ProjectDBAdaptor.QueryParams.ORGANISM_SCIENTIFIC_NAME.key(), updateParams.getOrganism().getScientificName());
                }
                params.remove("organism");
            }

            DataResult result = catalogManager.getProjectManager().update(projectStr, params, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}