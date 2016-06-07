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

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.execution.executors.ExecutorManager;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.analysis.storage.variant.VariantStorage;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
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


    public CohortWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                          @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @ApiOperation(value = "Create a cohort", position = 1, notes = "A cohort can be created by providing a list of SampleIds, " +
            "or providing a categorical variable (both variableSetId and variable). " +
            "If none of this is given, an empty cohort will be created.")
    public Response createCohort(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                 @ApiParam(value = "name", required = true) @QueryParam("name") String cohortName,
                                 @ApiParam(value = "type", required = false) @QueryParam("type") @DefaultValue("COLLECTION") Cohort.Type type,
                                 @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") long variableSetId,
                                 @ApiParam(value = "description", required = false) @QueryParam("description") String cohortDescription,
                                 @ApiParam(value = "sampleIds", required = false) @QueryParam("sampleIds") String sampleIdsStr,
                                 @ApiParam(value = "variable", required = false) @QueryParam("variable") String variableName) {
        try {
            //QueryOptions queryOptions = getAllQueryOptions();
            List<QueryResult<Cohort>> cohorts = new LinkedList<>();
            if (variableName != null && !variableName.isEmpty() && sampleIdsStr != null && !sampleIdsStr.isEmpty()) {
                return createErrorResponse("", "Can only create a cohort given list of sampleIds or a categorical variable name");
            }

            long studyId = catalogManager.getStudyId(studyIdStr);
            if (sampleIdsStr != null && !sampleIdsStr.isEmpty()) {
                QueryOptions samplesQOptions = new QueryOptions("include", "projects.studies.samples.id");
                Query samplesQuery = new Query("id", sampleIdsStr);
                cohorts.add(createCohort(studyId, cohortName, type, cohortDescription, samplesQuery, samplesQOptions));
            } else if (variableSetId > 0) {
                VariableSet variableSet = catalogManager.getVariableSet(variableSetId, null, sessionId).first();
                Variable variable = null;
                for (Variable v : variableSet.getVariables()) {
                    if (v.getId().equals(variableName)) {
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
                    Query samplesQuery = new Query(CatalogSampleDBAdaptor.QueryParams.ANNOTATION.key() + "." + variableName, s)
                            .append("variableSetId", variableSetId);
                    cohorts.add(createCohort(studyId, s, type, cohortDescription, samplesQuery, samplesQOptions));
                }
            } else {
                //Create empty cohort
                cohorts.add(catalogManager.createCohort(studyId, cohortName, type, cohortDescription, Collections.emptyList(), null, sessionId));
            }
            return createOkResponse(cohorts);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/info")
    @ApiOperation(value = "Get cohort information", position = 2)
    public Response infoSample(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") long cohortId) {
        try {
            QueryResult<Cohort> queryResult = catalogManager.getCohort(cohortId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{cohortId}/samples")
    @ApiOperation(value = "Get samples from cohort", position = 3)
    public Response getSamples(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") long cohortId) {
        try {
            Cohort cohort = catalogManager.getCohort(cohortId, queryOptions, sessionId).first();
            query.put("id", cohort.getSamples());
            long studyId = catalogManager.getStudyIdByCohortId(cohortId);
            QueryResult<Sample> allSamples = catalogManager.getAllSamples(studyId, query, queryOptions, sessionId);
            allSamples.setId("getCohortSamples");
            return createOkResponse(allSamples);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private QueryResult<Cohort> createCohort(long studyId, String cohortName, Cohort.Type type, String cohortDescription, Query query,
                                             QueryOptions queryOptions) throws CatalogException {
        QueryResult<Sample> queryResult = catalogManager.getAllSamples(studyId, query, queryOptions, sessionId);
        List<Long> sampleIds = new ArrayList<>(queryResult.getNumResults());
        sampleIds.addAll(queryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList()));
        return catalogManager.createCohort(studyId, cohortName, type, cohortDescription, sampleIds, null, sessionId);
    }

//
//    @GET
//    @Path("/create")
//    @Produces("application/json")
//    @ApiOperation(defaultValue = "Create a cohort")
//    public Response createCohort(
//            @ApiParam(defaultValue = "studyId", required = true) @QueryParam("studyId") int studyId,
//            @ApiParam(defaultValue = "cohortName", required = true) @QueryParam("cohortName") String cohortName,
//            @ApiParam(defaultValue = "cohortDescription", required = false) @QueryParam("cohortDescription") String cohortDescription) {
//        try {
//            QueryOptions queryOptions = getAllQueryOptions();
////            Object cohortInclude = queryOptions.remove("include");
////            Object cohortExclude = queryOptions.remove("exclude");
//            queryOptions.add("include", "projects.studies.samples.id");
//            QueryResult<Sample> queryResult = catalogManager.getAllSamples(studyId, queryOptions, sessionId);
//            queryOptions.remove("include");
//            List<Integer> sampleIds = new ArrayList<>(queryResult.getNumResults());
//            for (Sample sample : queryResult.getResult()) {
//                sampleIds.add(sample.getId());
//            }
//            QueryResult<Cohort> cohort = catalogManager.createCohort(studyId, cohortName, cohortDescription, sampleIds, null, sessionId);
//
//            return createOkResponse(cohort);
//        } catch (CatalogException e) {
//            e.printStackTrace();
//            return createErrorResponse(e.getMessage());
//        }
//    }
//

    @GET
    @Path("/{cohortId}/update")
    @ApiOperation(value = "Update some user attributes using GET method", position = 4)
    public Response update(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") long cohortId,
                           @ApiParam(value = "", required = false) @QueryParam("name") String name,
                           @ApiParam(value = "", required = false) @QueryParam("creationDate") String creationDate,
                           @ApiParam(value = "", required = false) @QueryParam("description") String description,
                           @ApiParam(value = "Comma separated values of sampleIds. Will replace all existing sampleIds", required = true) @QueryParam("samples") String samples) {
        try {
            // TODO: Change queryOptions, queryOptions
            return createOkResponse(catalogManager.modifyCohort(cohortId, queryOptions, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }    }

    @POST
    @Path("/{cohortId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some user attributes using POST method", position = 4)
    public Response updateByPost(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") long cohortId,
                                 @ApiParam(value = "params", required = true) Map<String, Object> params) {
        try {
            return createOkResponse(catalogManager.modifyCohort(cohortId, new ObjectMap(params), queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/stats")
    @ApiOperation(value = "Cohort stats", position = 2)
    public Response stats(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortIdsCsv,
                          @ApiParam(value = "Calculate cohort stats", required = false) @QueryParam("calculate") boolean calculate,
                          @ApiParam(value = "Delete stats [PENDING]", required = false) @QueryParam("delete") boolean delete,
                          @ApiParam(value = "Log level", required = false) @QueryParam("log") String logLevel,
                          @ApiParam(value = "Output directory", required = false) @QueryParam("outdirId") String outdirIdStr
                          ) {
        try {
            String[] split = cohortIdsCsv.split(",");
            List<Long> cohortIds = new ArrayList<>(split.length);
            for (String cohortIdStr : split) {
                cohortIds.add(Long.parseLong(cohortIdStr));
            }
            if (calculate) {
                VariantStorage variantStorage = new VariantStorage(catalogManager);
                Long outdirId = outdirIdStr == null ? null : catalogManager.getFileId(outdirIdStr);
                queryOptions.put(ExecutorManager.EXECUTE, false);
                queryOptions.add(AnalysisFileIndexer.LOG_LEVEL, logLevel);
                QueryResult<Job> jobQueryResult =
                        variantStorage.calculateStats(outdirId, cohortIds, sessionId, new QueryOptions(queryOptions));
                return createOkResponse(jobQueryResult);
            } else if (delete) {
                List<QueryResult<Cohort>> results = new LinkedList<>();
                for (Long cohortId : cohortIds) {
                    results.add(catalogManager.deleteCohort(cohortId, queryOptions, sessionId));
                }
                return createOkResponse(results);
            } else {
                long studyId = catalogManager.getStudyIdByCohortId(cohortIds.get(0));
                return createOkResponse(catalogManager.getAllCohorts(studyId,
                        new Query(CatalogCohortDBAdaptor.QueryParams.ID.key(), cohortIdsCsv), new QueryOptions(), sessionId).first());
            }

        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/delete")
    @ApiOperation(value = "Delete cohort.", position = 5)
    public Response deleteCohort(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") long cohortId) {
        try {
            return createOkResponse(catalogManager.deleteCohort(cohortId, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{cohortId}/annotate")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "annotate cohort", position = 6)
    public Response annotateSamplePOST(@ApiParam(value = "CohortID", required = true) @PathParam("cohortId") String cohortId,
                                       @ApiParam(value = "Annotation set name. Must be unique for the cohort", required = true) @QueryParam("annotateSetName") String annotateSetName,
                                       @ApiParam(value = "VariableSetId of the new annotation", required = false) @QueryParam("variableSetId") long variableSetId,
                                       @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update") @DefaultValue("false") boolean update,
                                       @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete") @DefaultValue("false") boolean delete,
                                       Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult;
            if (delete && update) {
                return createErrorResponse("Annotate cohort", "Unable to update and delete annotations at the same time");
            } else if (delete) {
                queryResult = catalogManager.deleteCohortAnnotation(cohortId, annotateSetName, sessionId);
            } else if (update) {
                queryResult = catalogManager.updateCohortAnnotation(cohortId, annotateSetName, annotations, sessionId);
            } else {
                queryResult = catalogManager.annotateCohort(cohortId, annotateSetName, variableSetId, annotations, Collections.emptyMap(),
                        sessionId);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/annotate")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Annotate cohort", position = 6)
    public Response annotateSampleGET(@ApiParam(value = "CohortID", required = true) @PathParam("cohortId") String cohortId,
                                      @ApiParam(value = "Annotation set name. Must be unique for the cohort", required = true) @QueryParam("annotateSetName") String annotateSetName,
                                      @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") long variableSetId,
                                      @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update") @DefaultValue("false") boolean update,
                                      @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete") @DefaultValue("false") boolean delete) {
        try {
            QueryResult<AnnotationSet> queryResult;

            if (delete && update) {
                return createErrorResponse("Annotate cohort", "Unable to update and delete annotations at the same time");
            } else if (delete) {
                queryResult = catalogManager.deleteCohortAnnotation(cohortId, annotateSetName, sessionId);
            } else {
                if (update) {
                    long cohortLongId = catalogManager.getCohortId(cohortId, sessionId);
                    for (AnnotationSet annotationSet : catalogManager.getCohort(cohortLongId, null, sessionId).first().getAnnotationSets()) {
                        if (annotationSet.getId().equals(annotateSetName)) {
                            variableSetId = annotationSet.getVariableSetId();
                        }
                    }
                }
                QueryResult<VariableSet> variableSetResult = catalogManager.getVariableSet(variableSetId, null, sessionId);
                if(variableSetResult.getResult().isEmpty()) {
                    return createErrorResponse("cohort - annotate", "VariableSet not found.");
                }
                Map<String, Object> annotations = variableSetResult.getResult().get(0).getVariables().stream()
                        .filter(variable -> params.containsKey(variable.getId()))
                        .collect(Collectors.toMap(Variable::getId, variable -> params.getFirst(variable.getId())));

                if (update) {
                    queryResult = catalogManager.updateCohortAnnotation(cohortId, annotateSetName, annotations, sessionId);
                } else {
                    queryResult = catalogManager.annotateCohort(cohortId, annotateSetName, variableSetId, annotations,
                            Collections.emptyMap(), sessionId);
                }
            }

            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortIds}/share")
    @ApiOperation(value = "Share cohorts with other members", position = 7)
    public Response share(@PathParam(value = "cohortIds") String cohortIds,
                          @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members,
                          @ApiParam(value = "Comma separated list of cohort permissions", required = false) @DefaultValue("") @QueryParam("permissions") String permissions,
                          @ApiParam(value = "Boolean indicating whether to allow the change of of permissions in case any member already had any", required = true) @DefaultValue("false") @QueryParam("override") boolean override) {
        try {
            return createOkResponse(catalogManager.shareCohorts(cohortIds, members, Arrays.asList(permissions.split(",")), override, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortIds}/unshare")
    @ApiOperation(value = "Remove the permissions for the list of members", position = 8)
    public Response unshare(@PathParam(value = "cohortIds") String cohortIds,
                            @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members,
                            @ApiParam(value = "Comma separated list of cohort permissions", required = false) @DefaultValue("") @QueryParam("permissions") String permissions) {
        try {
            return createOkResponse(catalogManager.unshareCohorts(cohortIds, members, permissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group cohorts by several fields", position = 24)
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("") @QueryParam("by") String by,
                            @ApiParam(value = "studyId", required = true) @DefaultValue("") @QueryParam("studyId") String studyStr,
                            @ApiParam(value = "Comma separated list of ids.", required = false) @DefaultValue("") @QueryParam("id") String ids,
                            @ApiParam(value = "Comma separated list of names.", required = false) @DefaultValue("") @QueryParam("name") String names,
                            @ApiParam(value = "Comma separated Type values.", required = false) @DefaultValue("") @QueryParam("type") String type,
                            @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") String status,
                            @ApiParam(value = "creationDate", required = false) @DefaultValue("") @QueryParam("creationDate") String creationDate,
                            @ApiParam(value = "Comma separated sampleIds", required = false) @DefaultValue("") @QueryParam("sampleIds") String sampleIds,
                            @ApiParam(value = "attributes", required = false) @DefaultValue("") @QueryParam("attributes") String attributes,
                            @ApiParam(value = "numerical attributes", required = false) @DefaultValue("") @QueryParam("nattributes") String nattributes) {
        try {
            Query query = new Query();
            QueryOptions qOptions = new QueryOptions();
            parseQueryParams(params, CatalogCohortDBAdaptor.QueryParams::getParam, query, qOptions);

            logger.debug("query = " + query.toJson());
            logger.debug("queryOptions = " + qOptions.toJson());
            QueryResult result = catalogManager.cohortGroupBy(query, qOptions, by, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
