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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.api.ICohortManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jacobo on 15/12/14.
 */
@Path("/{version}/cohorts")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Cohorts", position = 9, description = "Methods for working with 'cohorts' endpoint")
public class CohortWSServer extends OpenCGAWSServer {

    private ICohortManager cohortManager;

    public CohortWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(uriInfo, httpServletRequest);
        cohortManager = catalogManager.getCohortManager();
    }

    private Response createCohort(String studyStr, String cohortName, Study.Type type, long variableSetId, String cohortDescription,
                                  String sampleIdsStr, String variableName) {
        try {

            List<QueryResult<Cohort>> cohorts = new LinkedList<>();
            if (variableName != null && !variableName.isEmpty() && sampleIdsStr != null && !sampleIdsStr.isEmpty()) {
                return createErrorResponse("", "Can only create a cohort given list of sampleIds or a categorical "
                        + "variable name");
            }

            long studyId = catalogManager.getStudyId(studyStr, sessionId);
            if (sampleIdsStr != null && !sampleIdsStr.isEmpty()) {
                String userId = catalogManager.getUserManager().getId(sessionId);
                List<Long> sampleIds = catalogManager.getSampleManager().getIds(userId, sampleIdsStr);
                QueryResult<Cohort> cohortQueryResult =
                        catalogManager.createCohort(studyId, cohortName, type, cohortDescription, sampleIds, null, sessionId);
                cohorts.add(cohortQueryResult);
            } else if (variableSetId > 0) {
                VariableSet variableSet = catalogManager.getVariableSet(variableSetId, null, sessionId).first();
                Variable variable = null;
                for (Variable v : variableSet.getVariables()) {
                    if (v.getName().equals(variableName)) {
                        variable = v;
                        break;
                    }
                }
                if (variable == null) {
                    return createErrorResponse("", "Variable " + variableName + " does not exist in variableSet " + variableSet.getName());
                }
                if (variable.getType() != Variable.VariableType.CATEGORICAL) {
                    return createErrorResponse("", "Can only create cohorts by variable, when is a categorical variable");
                }
                for (String s : variable.getAllowedValues()) {
                    QueryOptions samplesQOptions = new QueryOptions("include", "projects.studies.samples.id");
                    Query samplesQuery = new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key() + "." + variableName, s)
                            .append("variableSetId", variableSetId);

                    cohorts.add(createCohort(studyId, cohortName + "_" + s, type, cohortDescription, samplesQuery, samplesQOptions));
                }
            } else {
                //Create empty cohort
                cohorts.add(catalogManager.createCohort(studyId, cohortName, type, cohortDescription, Collections.emptyList(), null,
                        sessionId));
            }
            return createOkResponse(cohorts);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/create")
    @ApiOperation(value = "Create a cohort", position = 1, notes = "A cohort can be created by providing a list of SampleIds, " +
            "or providing a categorical variable (both variableSetId and variable). " +
            "If none of this is given, an empty cohort will be created.", response = Cohort.class)
    public Response createCohort(@ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId")
                                             String studyIdStr,
                                 @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                 @QueryParam("study") String studyStr,
                                 @ApiParam(value = "Name of the cohort.", required = true) @QueryParam ("name") String cohortName,
                                 @ApiParam(value = "type", required = false) @QueryParam("type") @DefaultValue("COLLECTION")
                                             Study.Type type,
                                 @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") long variableSetId,
                                 @ApiParam(value = "description", required = false) @QueryParam("description") String cohortDescription,
                                 @ApiParam(value = "(DEPRECATED) sampleIds", hidden = true) @QueryParam("sampleIds") String sampleIdsStr,
                                 @ApiParam(value = "Samples", required = false) @QueryParam("samples") String samplesStr,
                                 @ApiParam(value = "Variable name", required = false) @QueryParam("variable") String variableName) {

        if (StringUtils.isNotEmpty(studyIdStr)) {
            studyStr = studyIdStr;
        }
        if (StringUtils.isNotEmpty(sampleIdsStr)) {
            samplesStr = sampleIdsStr;
        }
        return createCohort(studyStr, cohortName, type, variableSetId, cohortDescription, samplesStr, variableName);
    }

    private static class CohortParameters {
        public String name;
        public Study.Type type;
        public String description;
        public String samples;
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create a cohort", position = 1, notes = "A cohort can be created by providing a list of SampleIds, " +
            "or providing a categorical variable (both variableSetId and variable). " +
            "If none of this is given, an empty cohort will be created.", response = Cohort.class)
    public Response createCohort(
            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "variableSetId") @QueryParam("variableSetId") long variableSetId,
            @ApiParam(value = "Variable name") @QueryParam("variable") String variableName,
            @ApiParam(value="JSON containing cohort information", required = true) CohortParameters params) {
        if (StringUtils.isNotEmpty(studyIdStr)) {
            studyStr = studyIdStr;
        }

        return createCohort(studyStr, params.name, params.type, variableSetId, params.description, params.samples, variableName);
    }

    @GET
    @Path("/{cohortId}/info")
    @ApiOperation(value = "Get cohort information", position = 2, response = Cohort.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response infoSample(@ApiParam(value = "Comma separated list of cohort names or ids", required = true) @PathParam("cohortId")
                                           String cohortStr,
                               @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                        @QueryParam("study") String studyStr) {
        try {
            try {
                List<QueryResult<Cohort>> queryResults = new LinkedList<>();
                List<Long> cohortIds = cohortManager.getIds(cohortStr, studyStr, sessionId).getResourceIds();
                for (Long cohortId : cohortIds) {
                    queryResults.add(catalogManager.getCohort(cohortId, queryOptions, sessionId));
                }
                return createOkResponse(queryResults);
            } catch (Exception e) {
                return createErrorResponse(e);
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search cohorts", position = 2, response = Cohort.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response searchCohorts(@ApiParam(value = "Study [[user@]project:]study where study and project can be either "
            + "the id or alias") @QueryParam("study") String studyStr,
                                  @ApiParam(value = "Name of the cohort") @QueryParam("name") String name,
                                  @ApiParam(value = "Cohort type") @QueryParam("type") Study.Type type,
                                  @ApiParam(value = "Status") @QueryParam("status") String status,
                                  @ApiParam(value = "Sample list") @QueryParam("samples") String samplesStr) {
//                                  @ApiParam(value = "Family") @QueryParam("family") String family) {
        try {
            if (StringUtils.isNotEmpty(samplesStr)) {
                // First look for the sample ids.
                AbstractManager.MyResourceIds samples =
                        catalogManager.getSampleManager().getIds(samplesStr, studyStr, sessionId);
                query.remove("samples");
                query.append(CohortDBAdaptor.QueryParams.SAMPLES.key(), samples.getResourceIds());
            }
            return createOkResponse(catalogManager.getCohortManager().search(studyStr, query, queryOptions, sessionId));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/samples")
    @ApiOperation(value = "Get samples from cohort", position = 3, response = Sample[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getSamples(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortStr,
                               @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                    @QueryParam("study") String studyStr) {
        try {
            AbstractManager.MyResourceId resource = cohortManager.getId(cohortStr, studyStr, sessionId);
            long cohortId = resource.getResourceId();
            Cohort cohort = catalogManager.getCohort(cohortId, queryOptions, sessionId).first();
            if (cohort.getSamples() == null || cohort.getSamples().size() == 0) {
                return createOkResponse(new QueryResult<>("Samples from cohort " + cohortStr, -1, 0, 0, "The cohort has no samples", "",
                        Collections.emptyList()));
            }
            long studyId = resource.getStudyId();
            query = new Query(SampleDBAdaptor.QueryParams.ID.key(), cohort.getSamples());
            QueryResult<Sample> allSamples = catalogManager.getAllSamples(studyId, query, queryOptions, sessionId);
            allSamples.setId("Samples from cohort " + cohortStr);
            return createOkResponse(allSamples);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private QueryResult<Cohort> createCohort(long studyId, String cohortName, Study.Type type, String cohortDescription, Query query,
                                             QueryOptions queryOptions) throws CatalogException {
        //TODO CHANGE THIS for can insert the name also id(number)
        QueryResult<Sample> queryResult = catalogManager.getAllSamples(studyId, query, queryOptions, sessionId);
        List<Long> sampleIds = new ArrayList<>(queryResult.getNumResults());
        sampleIds.addAll(queryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList()));
        //TODO FOR THIS. Its possible change the param query to a String
        //List<QueryResult<Sample>> queryResults = new LinkedList<>();
        //List<Long> sampleIds = catalogManager.getSampleIds(query.get("id").toString(), sessionId);
        return catalogManager.createCohort(studyId, cohortName, type, cohortDescription, sampleIds, null, sessionId);
    }

    @GET
    @Path("/{cohort}/update")
    @ApiOperation(value = "Update some user attributes using GET method", position = 4, response = Cohort.class)
    public Response update(@ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortStr,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr,
                           @ApiParam(value = "") @QueryParam("name") String name,
                           @ApiParam(value = "") @QueryParam("creationDate") String creationDate,
                           @ApiParam(value = "") @QueryParam("description") String description,
                           @ApiParam(value = "Comma separated values of sampleIds. Will replace all existing sampleIds")
                               @QueryParam("samples") String samples) {
        try {
            long cohortId = cohortManager.getId(cohortStr, studyStr, sessionId).getResourceId();
            query.remove(CohortDBAdaptor.QueryParams.STUDY.key());
            // TODO: Change queryOptions, queryOptions
            return createOkResponse(catalogManager.modifyCohort(cohortId, query, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }    }

    @POST
    @Path("/{cohort}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some user attributes using POST method", position = 4)
    public Response updateByPost(@ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortStr,
                                 @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                        @QueryParam("study") String studyStr,
                                 @ApiParam(value = "params", required = true) Map<String, Object> params) {
        try {
            long cohortId = cohortManager.getId(cohortStr, studyStr, sessionId).getResourceId();
            return createOkResponse(catalogManager.modifyCohort(cohortId, new ObjectMap(params), queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohort}/delete")
    @ApiOperation(value = "Delete cohort.", position = 5)
    public Response deleteCohort(@ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortStr,
                                 @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                        @QueryParam("study") String studyStr) {
        try {
//            long cohortId = catalogManager.getCohortId(cohortStr, sessionId);
            List<QueryResult<Cohort>> delete = cohortManager.delete(cohortStr, studyStr, queryOptions, sessionId);
            return createOkResponse(delete);
        } catch (CatalogException | IOException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohort}/annotationsets/search")
    @ApiOperation(value = "Search annotation sets [NOT TESTED]", position = 11)
    public Response searchAnnotationSetGET(@ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortStr,
                                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or"
                                                   + " alias") @QueryParam("study") String studyStr,
                                           @ApiParam(value = "variableSetId", required = true) @QueryParam("variableSetId")
                                                       long variableSetId,
                                           @ApiParam(value = "annotation", required = false) @QueryParam("annotation") String annotation,
                                           @ApiParam(value = "Indicates whether to show the annotations as key-value",
                                                   defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(cohortManager.searchAnnotationSetAsMap(cohortStr, studyStr, variableSetId, annotation, sessionId));
            } else {
                return createOkResponse(cohortManager.searchAnnotationSet(cohortStr, studyStr, variableSetId, annotation, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohort}/annotationsets/info")
    @ApiOperation(value = "Return all the annotation sets of the cohort [NOT TESTED]", position = 12)
    public Response infoAnnotationSetGET(@ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortStr,
                                         @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or"
                                                 + " alias") @QueryParam("study") String studyStr,
                                         @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false")
                                             @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(cohortManager.getAllAnnotationSetsAsMap(cohortStr, studyStr, sessionId));
            } else {
                return createOkResponse(cohortManager.getAllAnnotationSets(cohortStr, studyStr, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @ApiModel
    public static class AnnotationsetParameters {
        @ApiModelProperty(required = true)
        public String name;

        @ApiModelProperty
        public Map<String, Object> annotations;
    }

    @POST
    @Path("/{cohort}/annotationsets/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create an annotation set for the cohort [NOT TESTED]", position = 13)
    public Response annotateSamplePOST(
            @ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "VariableSetId of the new annotation", required = true) @QueryParam("variableSetId") long variableSetId,
            @ApiParam(value="JSON containing the annotation set name and the array of annotations. The name should be unique for the "
                    + "cohort", required = true) AnnotationsetParameters params) {
        try {
            QueryResult<AnnotationSet> queryResult = cohortManager.createAnnotationSet(cohortStr, studyStr, variableSetId,
                    params.name, params.annotations, Collections.emptyMap(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohort}/annotationsets/{annotationsetName}/delete")
    @ApiOperation(value = "Delete the annotation set or the annotations within the annotation set [NOT TESTED]", position = 14)
    public Response deleteAnnotationGET(@ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortStr,
                                        @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                                + "alias") @QueryParam("study") String studyStr,
                                        @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName")
                                                    String annotationsetName,
                                        @ApiParam(value = "[NOT IMPLEMENTED] Comma separated list of annotation names to be deleted",
                                                required = false) @QueryParam("annotations") String annotations) {
        try {
            QueryResult<AnnotationSet> queryResult;
            if (annotations != null) {
                queryResult = cohortManager.deleteAnnotations(cohortStr, studyStr, annotationsetName, annotations, sessionId);
            } else {
                queryResult = cohortManager.deleteAnnotationSet(cohortStr, studyStr, annotationsetName, sessionId);
            }
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{cohort}/annotationsets/{annotationsetName}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update the annotations [NOT TESTED]", position = 15)
    public Response updateAnnotationGET(
            @ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName") String annotationsetName,
            Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult = cohortManager.updateAnnotationSet(cohortIdStr, studyStr, annotationsetName,
                    annotations, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohort}/annotationsets/{annotationsetName}/info")
    @ApiOperation(value = "Return the annotation set [NOT TESTED]", position = 16)
    public Response infoAnnotationGET(@ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortStr,
                                      @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                              + "alias") @QueryParam("study") String studyStr,
                                      @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName")
                                                  String annotationsetName,
                                      @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false")
                                          @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(cohortManager.getAnnotationSetAsMap(cohortStr, studyStr, annotationsetName, sessionId));
            } else {
                return createOkResponse(cohortManager.getAnnotationSet(cohortStr, studyStr, annotationsetName, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group cohorts by several fields", position = 24)
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
                                @QueryParam("fields") String fields,
                            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @DefaultValue("") @QueryParam("studyId")
                                    String studyIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr,
                            @ApiParam(value = "Comma separated list of ids.", required = false) @DefaultValue("") @QueryParam("id")
                                        String ids,
                            @ApiParam(value = "Comma separated list of names.", required = false) @DefaultValue("") @QueryParam("name")
                                        String names,
                            @ApiParam(value = "Comma separated Type values.", required = false) @DefaultValue("") @QueryParam("type")
                                        String type,
                            @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") String status,
                            @ApiParam(value = "creationDate", required = false) @DefaultValue("") @QueryParam("creationDate")
                                        String creationDate,
                            @ApiParam(value = "Comma separated sampleIds", required = false) @DefaultValue("") @QueryParam("sampleIds")
                                        String sampleIds,
                            @ApiParam(value = "attributes", required = false) @DefaultValue("") @QueryParam("attributes") String attributes,
                            @ApiParam(value = "numerical attributes", required = false) @DefaultValue("") @QueryParam("nattributes")
                                        String nattributes) {
        try {
            QueryResult result = cohortManager.groupBy(studyIdStr, query, queryOptions, fields, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohorts}/acl")
    @ApiOperation(value = "Return the acl of the cohort", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of cohort ids", required = true) @PathParam("cohorts")
                                        String cohortIdsStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr) {
        try {
            return createOkResponse(catalogManager.getAllCohortAcls(cohortIdsStr, studyStr, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{cohorts}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of members", position = 19)
    public Response createRole(@ApiParam(value = "Comma separated list of cohort ids", required = true) @PathParam("cohorts")
                                           String cohortIdsStr,
                               @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                    @QueryParam("study") String studyStr,
                               @ApiParam(value = "Comma separated list of permissions that will be granted to the member list")
                                   @DefaultValue("") @QueryParam("permissions") String permissions,
                               @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'",
                                       required = true) @DefaultValue("") @QueryParam("members") String members) {
        try {
            return createOkResponse(catalogManager.createCohortAcls(cohortIdsStr, studyStr, members, permissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{cohorts}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of members", position = 19)
    public Response createRolePOST(
            @ApiParam(value = "Comma separated list of cohort ids", required = true) @PathParam("cohorts") String cohortIdsStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value="JSON containing the parameters defined in GET. Mandatory keys: 'members'", required = true)
                    StudyWSServer.CreateAclCommands params) {
        try {
            return createOkResponse(catalogManager.createCohortAcls(cohortIdsStr, studyStr, params.members, params.permissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohort}/acl/{memberId}/info")
    @ApiOperation(value = "Return the set of permissions granted for the member", position = 20)
    public Response getAcl(@ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortIdStr,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr,
                           @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.getCohortAcl(cohortIdStr, studyStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohort}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(@ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortIdStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                    @QueryParam("study") String studyStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
                              @ApiParam(value = "Comma separated list of permissions to add", required = false)
                                  @QueryParam("addPermissions") String addPermissions,
                              @ApiParam(value = "Comma separated list of permissions to remove", required = false)
                                  @QueryParam("removePermissions") String removePermissions,
                              @ApiParam(value = "Comma separated list of permissions to set", required = false)
                                  @QueryParam("setPermissions") String setPermissions) {
        try {
            return createOkResponse(catalogManager.updateCohortAcl(cohortIdStr, studyStr, memberId, addPermissions, removePermissions,
                    setPermissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{cohort}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(
            @ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value="JSON containing one of the keys 'addPermissions', 'setPermissions' or 'removePermissions'", required = true)
                    StudyWSServer.MemberAclUpdate params) {
        try {
            return createOkResponse(catalogManager.updateCohortAcl(cohortIdStr, studyStr, memberId, params.addPermissions,
                    params.removePermissions, params.setPermissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohort}/acl/{memberId}/delete")
    @ApiOperation(value = "Delete all the permissions granted for the member", position = 22)
    public Response deleteAcl(@ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortIdStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                    @QueryParam("study") String studyStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.removeCohortAcl(cohortIdStr, studyStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
