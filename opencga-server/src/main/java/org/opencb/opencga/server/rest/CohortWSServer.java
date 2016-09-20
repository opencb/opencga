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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.monitor.executors.old.ExecutorManager;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.analysis.storage.variant.VariantStorage;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
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


    public CohortWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @ApiOperation(value = "Create a cohort", position = 1, notes = "A cohort can be created by providing a list of SampleIds, " +
            "or providing a categorical variable (both variableSetId and variable). " +
            "If none of this is given, an empty cohort will be created.", response = Cohort.class)
    public Response createCohort(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                 @ApiParam(value = "name", required = true) @QueryParam("name") String cohortName,
                                 @ApiParam(value = "type", required = false) @QueryParam("type") @DefaultValue("COLLECTION") Study.Type type,
                                 @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") long variableSetId,
                                 @ApiParam(value = "description", required = false) @QueryParam("description") String cohortDescription,
                                 @ApiParam(value = "sampleIds", required = false) @QueryParam("sampleIds") String sampleIdsStr,
                                 @ApiParam(value = "Variable name", required = false) @QueryParam("variable") String variableName) {
        try {
            //QueryOptions queryOptions = getAllQueryOptions();
            List<QueryResult<Cohort>> cohorts = new LinkedList<>();
            if (variableName != null && !variableName.isEmpty() && sampleIdsStr != null && !sampleIdsStr.isEmpty()) {
                return createErrorResponse("", "Can only create a cohort given list of sampleIds or a categorical variable name");
            }

            long studyId = catalogManager.getStudyId(studyIdStr, sessionId);
            if (sampleIdsStr != null && !sampleIdsStr.isEmpty()) {
                QueryOptions samplesQOptions = new QueryOptions("include", "projects.studies.samples.id");
                Query samplesQuery = new Query("id", sampleIdsStr);
                cohorts.add(createCohort(studyId, cohortName, type, cohortDescription, samplesQuery, samplesQOptions));
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
    @ApiOperation(value = "Get cohort information", position = 2, response = Cohort.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response infoSample(@ApiParam(value = "Comma separated list of cohort names or ids", required = true) @PathParam("cohortId") String cohortStr) {
        try {
            try {
                List<QueryResult<Cohort>> queryResults = new LinkedList<>();
                List<Long> cohortIds = catalogManager.getCohortIds(cohortStr, sessionId);
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
    @Path("/{cohortId}/samples")
    @ApiOperation(value = "Get samples from cohort", position = 3, response = Sample[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getSamples(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortStr) {
        try {
            long cohortId = catalogManager.getCohortId(cohortStr, sessionId);
            Cohort cohort = catalogManager.getCohort(cohortId, queryOptions, sessionId).first();
            if (cohort.getSamples() == null || cohort.getSamples().size() == 0) {
                return createOkResponse(new QueryResult<>("Samples from cohort " + cohortStr, -1, 0, 0, "The cohort has no samples", "", Collections.emptyList()));
            }
            long studyId = catalogManager.getStudyIdByCohortId(cohortId);
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
    @ApiOperation(value = "Update some user attributes using GET method", position = 4, response = Cohort.class)
    public Response update(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortStr,
                           @ApiParam(value = "", required = false) @QueryParam("name") String name,
                           @ApiParam(value = "", required = false) @QueryParam("creationDate") String creationDate,
                           @ApiParam(value = "", required = false) @QueryParam("description") String description,
                           @ApiParam(value = "Comma separated values of sampleIds. Will replace all existing sampleIds", required = false) @QueryParam("samples") String samples) {
        try {
            long cohortId = catalogManager.getCohortId(cohortStr, sessionId);
            // TODO: Change queryOptions, queryOptions
            return createOkResponse(catalogManager.modifyCohort(cohortId, queryOptions, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }    }

    @POST
    @Path("/{cohortId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some user attributes using POST method", position = 4)
    public Response updateByPost(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortStr,
                                 @ApiParam(value = "params", required = true) Map<String, Object> params) {
        try {
            long cohortId = catalogManager.getCohortId(cohortStr, sessionId);
            return createOkResponse(catalogManager.modifyCohort(cohortId, new ObjectMap(params), queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/stats")
    @ApiOperation(value = "Cohort stats", position = 2)
    public Response stats(@ApiParam(value = "Comma separated list of cohort names or ids", required = true) @PathParam("cohortId") String cohortIdsCsv,
                          @ApiParam(value = "Calculate cohort stats", required = false) @QueryParam("calculate") boolean calculate,
                          @ApiParam(value = "Delete stats [PENDING]", required = false) @QueryParam("delete") boolean delete,
                          @ApiParam(value = "Log level", required = false) @QueryParam("log") String logLevel,
                          @ApiParam(value = "Output directory", required = false) @QueryParam("outdirId") String outdirIdStr
                          ) {
        try {
            List<Long> cohortIds = catalogManager.getCohortIds(cohortIdsCsv, sessionId);
            if (calculate) {
                VariantStorage variantStorage = new VariantStorage(catalogManager);
                Long outdirId = outdirIdStr == null ? null : catalogManager.getFileId(outdirIdStr, sessionId);
                queryOptions.put(ExecutorManager.EXECUTE, false);
                queryOptions.add(AnalysisFileIndexer.LOG_LEVEL, logLevel);
                QueryResult<Job> jobQueryResult =
                        variantStorage.calculateStats(outdirId, cohortIds, sessionId, new QueryOptions(queryOptions));
                return createOkResponse(jobQueryResult);
            } else if (delete) {
                List<QueryResult<Cohort>> results = catalogManager.getCohortManager().delete(cohortIdsCsv, queryOptions, sessionId);
                return createOkResponse(results);
            } else {
                long studyId = catalogManager.getStudyIdByCohortId(cohortIds.get(0));
                return createOkResponse(catalogManager.getAllCohorts(studyId,
                        new Query(CohortDBAdaptor.QueryParams.ID.key(), cohortIdsCsv), new QueryOptions(), sessionId).first());
            }

        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/delete")
    @ApiOperation(value = "Delete cohort.", position = 5)
    public Response deleteCohort(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortStr) {
        try {
//            long cohortId = catalogManager.getCohortId(cohortStr, sessionId);
            List<QueryResult<Cohort>> delete = catalogManager.getCohortManager().delete(cohortStr, queryOptions, sessionId);
            return createOkResponse(delete);
        } catch (CatalogException | IOException e) {
            return createErrorResponse(e);
        }
    }
//    @Deprecated
//    @POST
//    @Path("/{cohortId}/annotate")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "annotate cohort [DEPRECATED]", position = 6)
//    public Response annotateSamplePOST(@ApiParam(value = "CohortID", required = true) @PathParam("cohortId") String cohortId,
//                                       @ApiParam(value = "Annotation set name. Must be unique for the cohort", required = true) @QueryParam("annotateSetName") String annotateSetName,
//                                       @ApiParam(value = "VariableSetId of the new annotation", required = false) @QueryParam("variableSetId") long variableSetId,
//                                       @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update") @DefaultValue("false") boolean update,
//                                       @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete") @DefaultValue("false") boolean delete,
//                                       Map<String, Object> annotations) {
//        try {
//            QueryResult<AnnotationSet> queryResult;
//            if (delete && update) {
//                return createErrorResponse("Annotate cohort", "Unable to update and delete annotations at the same time");
//            } else if (delete) {
//                queryResult = catalogManager.deleteCohortAnnotation(cohortId, annotateSetName, sessionId);
//            } else if (update) {
//                queryResult = catalogManager.updateCohortAnnotation(cohortId, annotateSetName, annotations, sessionId);
//            } else {
//                queryResult = catalogManager.annotateCohort(cohortId, annotateSetName, variableSetId, annotations, Collections.emptyMap(),
//                        sessionId);
//            }
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//    @Deprecated
//    @GET
//    @Path("/{cohortId}/annotate")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "Annotate cohort[DEPRECATED]", position = 6)
//    public Response annotateSampleGET(@ApiParam(value = "CohortID", required = true) @PathParam("cohortId") String cohortId,
//                                      @ApiParam(value = "Annotation set name. Must be unique for the cohort", required = true) @QueryParam("annotateSetName") String annotateSetName,
//                                      @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") long variableSetId,
//                                      @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update") @DefaultValue("false") boolean update,
//                                      @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete") @DefaultValue("false") boolean delete) {
//        try {
//            QueryResult<AnnotationSet> queryResult;
//
//            if (delete && update) {
//                return createErrorResponse("Annotate cohort", "Unable to update and delete annotations at the same time");
//            } else if (delete) {
//                queryResult = catalogManager.deleteCohortAnnotation(cohortId, annotateSetName, sessionId);
//            } else {
//                if (update) {
//                    long cohortLongId = catalogManager.getCohortId(cohortId, sessionId);
//                    for (AnnotationSet annotationSet : catalogManager.getCohort(cohortLongId, null, sessionId).first().getAnnotationSets()) {
//                        if (annotationSet.getName().equals(annotateSetName)) {
//                            variableSetId = annotationSet.getVariableSetId();
//                        }
//                    }
//                }
//                QueryResult<VariableSet> variableSetResult = catalogManager.getVariableSet(variableSetId, null, sessionId);
//                if(variableSetResult.getResult().isEmpty()) {
//                    return createErrorResponse("cohort - annotate", "VariableSet not found.");
//                }
//                Map<String, Object> annotations = variableSetResult.getResult().get(0).getVariables().stream()
//                        .filter(variable -> params.containsKey(variable.getName()))
//                        .collect(Collectors.toMap(Variable::getName, variable -> params.getFirst(variable.getName())));
//
//                if (update) {
//                    queryResult = catalogManager.updateCohortAnnotation(cohortId, annotateSetName, annotations, sessionId);
//                } else {
//                    queryResult = catalogManager.annotateCohort(cohortId, annotateSetName, variableSetId, annotations,
//                            Collections.emptyMap(), sessionId);
//                }
//            }
//
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @GET
    @Path("/{cohortId}/annotationSets/search")
    @ApiOperation(value = "Search annotation sets [NOT TESTED]", position = 11)
    public Response searchAnnotationSetGET(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortStr,
                                           @ApiParam(value = "variableSetId", required = true) @QueryParam("variableSetId") long variableSetId,
                                           @ApiParam(value = "annotation", required = false) @QueryParam("annotation") String annotation,
                                           @ApiParam(value = "Indicates whether to show the annotations as key-value", required = false, defaultValue = "true") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(catalogManager.getCohortManager().searchAnnotationSetAsMap(cohortStr, variableSetId, annotation, sessionId));
            } else {
                return createOkResponse(catalogManager.getCohortManager().searchAnnotationSet(cohortStr, variableSetId, annotation, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/annotationSets/info")
    @ApiOperation(value = "Return all the annotation sets of the cohort [NOT TESTED]", position = 12)
    public Response infoAnnotationSetGET(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortStr,
                                         @ApiParam(value = "Indicates whether to show the annotations as key-value", required = false, defaultValue = "true") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(catalogManager.getCohortManager().getAllAnnotationSetsAsMap(cohortStr, sessionId));
            } else {
                return createOkResponse(catalogManager.getCohortManager().getAllAnnotationSets(cohortStr, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{cohortId}/annotationSets/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create an annotation set for the cohort [NOT TESTED]", position = 13)
    public Response annotateSamplePOST(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortStr,
                                       @ApiParam(value = "VariableSetId of the new annotation", required = true) @QueryParam("variableSetId") long variableSetId,
                                       @ApiParam(value = "Annotation set name. Must be unique for the sample", required = true) @QueryParam("annotateSetName") String annotateSetName,
                                       Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult = catalogManager.createCohortAnnotationSet(cohortStr, variableSetId,
                    annotateSetName, annotations, Collections.emptyMap(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/annotationSets/{annotationSetName}/delete")
    @ApiOperation(value = "Delete the annotation set or the annotations within the annotation set [NOT TESTED]", position = 14)
    public Response deleteAnnotationGET(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortStr,
                                        @ApiParam(value = "annotationSetName", required = true) @PathParam("annotationSetName") String annotationSetName,
                                        @ApiParam(value = "[NOT IMPLEMENTED] Comma separated list of annotation names to be deleted", required = false) @QueryParam("annotations") String annotations) {
        try {
            QueryResult<AnnotationSet> queryResult;
            if (annotations != null) {
                queryResult = catalogManager.deleteCohortAnnotations(cohortStr, annotationSetName, annotations, sessionId);
            } else {
                queryResult = catalogManager.deleteCohortAnnotationSet(cohortStr, annotationSetName, sessionId);
            }
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{cohortId}/annotationSets/{annotationSetName}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update the annotations [NOT TESTED]", position = 15)
    public Response updateAnnotationGET(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortIdStr,
                                        @ApiParam(value = "annotationSetName", required = true) @PathParam("annotationSetName") String annotationSetName,
//                                        @ApiParam(value = "variableSetId", required = true) @QueryParam("variableSetId") long variableSetId,
//                                        @ApiParam(value = "reset", required = false) @QueryParam("reset") String reset,
                                        Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult = catalogManager.updateCohortAnnotationSet(cohortIdStr, annotationSetName,
                    annotations, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/annotationSets/{annotationSetName}/info")
    @ApiOperation(value = "Return the annotation set [NOT TESTED]", position = 16)
    public Response infoAnnotationGET(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortStr,
                                      @ApiParam(value = "annotationSetName", required = true) @PathParam("annotationSetName") String annotationSetName,
                                      @ApiParam(value = "Indicates whether to show the annotations as key-value", required = false, defaultValue = "true") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(catalogManager.getCohortManager().getAnnotationSetAsMap(cohortStr, annotationSetName, sessionId));
            } else {
                return createOkResponse(catalogManager.getCohortManager().getAnnotationSet(cohortStr, annotationSetName, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }
//
//    @GET
//    @Path("/{cohortIds}/share")
//    @ApiOperation(value = "Share cohorts with other members", position = 7)
//    public Response share(@PathParam(value = "cohortIds") String cohortIds,
//                          @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members,
//                          @ApiParam(value = "Comma separated list of cohort permissions", required = false) @DefaultValue("") @QueryParam("permissions") String permissions,
//                          @ApiParam(value = "Boolean indicating whether to allow the change of of permissions in case any member already had any", required = true) @DefaultValue("false") @QueryParam("override") boolean override) {
//        try {
//            return createOkResponse(catalogManager.shareCohorts(cohortIds, members, Arrays.asList(permissions.split(",")), override, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    @GET
//    @Path("/{cohortIds}/unshare")
//    @ApiOperation(value = "Remove the permissions for the list of members", position = 8)
//    public Response unshare(@PathParam(value = "cohortIds") String cohortIds,
//                            @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members,
//                            @ApiParam(value = "Comma separated list of cohort permissions", required = false) @DefaultValue("") @QueryParam("permissions") String permissions) {
//        try {
//            return createOkResponse(catalogManager.unshareCohorts(cohortIds, members, permissions, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group cohorts by several fields", position = 24)
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("") @QueryParam("fields") String fields,
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
            parseQueryParams(params, CohortDBAdaptor.QueryParams::getParam, query, qOptions);

            logger.debug("query = " + query.toJson());
            logger.debug("queryOptions = " + qOptions.toJson());
            QueryResult result = catalogManager.cohortGroupBy(query, qOptions, fields, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortIds}/acl")
    @ApiOperation(value = "Return the acl of the cohort", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of cohort ids", required = true) @PathParam("cohortIds") String cohortIdsStr) {
        try {
            return createOkResponse(catalogManager.getAllCohortAcls(cohortIdsStr, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{cohortIds}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of members", position = 19)
    public Response createRole(@ApiParam(value = "Comma separated list of cohort ids", required = true) @PathParam("cohortIds") String cohortIdsStr,
                               @ApiParam(value = "Comma separated list of permissions that will be granted to the member list", required = false) @DefaultValue("") @QueryParam("permissions") String permissions,
                               @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members) {
        try {
            return createOkResponse(catalogManager.createCohortAcls(cohortIdsStr, members, permissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/acl/{memberId}/info")
    @ApiOperation(value = "Return the set of permissions granted for the member", position = 20)
    public Response getAcl(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortIdStr,
                           @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.getCohortAcl(cohortIdStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortIdStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
                              @ApiParam(value = "Comma separated list of permissions to add", required = false) @QueryParam("addPermissions") String addPermissions,
                              @ApiParam(value = "Comma separated list of permissions to remove", required = false) @QueryParam("removePermissions") String removePermissions,
                              @ApiParam(value = "Comma separated list of permissions to set", required = false) @QueryParam("setPermissions") String setPermissions) {
        try {
            return createOkResponse(catalogManager.updateCohortAcl(cohortIdStr, memberId, addPermissions, removePermissions, setPermissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/acl/{memberId}/delete")
    @ApiOperation(value = "Delete all the permissions granted for the member", position = 22)
    public Response deleteAcl(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortIdStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.removeIndividualAcl(cohortIdStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
