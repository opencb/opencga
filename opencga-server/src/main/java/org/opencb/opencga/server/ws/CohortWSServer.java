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
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.ToolManager;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.analysis.storage.variant.VariantStorage;
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
@Api(value = "Cohorts", position = 8, description = "Methods for working with 'cohorts' endpoint")
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
                                 @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") int variableSetId,
                                 @ApiParam(value = "description", required = false) @QueryParam("description") String cohortDescription,
                                 @ApiParam(value = "sampleIds", required = false) @QueryParam("sampleIds") String sampleIdsStr,
                                 @ApiParam(value = "variable", required = false) @QueryParam("variable") String variableName) {
        try {
            //QueryOptions queryOptions = getAllQueryOptions();
            List<QueryResult<Cohort>> cohorts = new LinkedList<>();
            if (variableName != null && !variableName.isEmpty() && sampleIdsStr != null && !sampleIdsStr.isEmpty()) {
                return createErrorResponse("", "Can only create a cohort given list of sampleIds or a categorical variable name");
            }

            int studyId = catalogManager.getStudyId(studyIdStr);
            if (sampleIdsStr != null && !sampleIdsStr.isEmpty()) {
                QueryOptions samplesQuery = new QueryOptions("include", "projects.studies.samples.id");
                samplesQuery.add("id", sampleIdsStr);
                cohorts.add(createCohort(studyId, cohortName, type, cohortDescription, samplesQuery));
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
                    return createErrorResponse("", "Variable " + variable  + " does not exist. ");
                }
                if (variable.getType() != Variable.VariableType.CATEGORICAL) {
                    return createErrorResponse("", "Can only create cohorts by variable, when is a categorical variable");
                }
                for (String s : variable.getAllowedValues()) {
                    QueryOptions samplesQuery = new QueryOptions("include", "projects.studies.samples.id");
                    samplesQuery.add("annotation", variableName + ":" + s);
                    samplesQuery.add("variableSetId", variableSetId);
                    cohorts.add(createCohort(studyId, s, type, cohortDescription, samplesQuery));
                }
            } else {
                //Create empty cohort
                cohorts.add(catalogManager.createCohort(studyId, cohortName, type, cohortDescription, Collections.<Integer>emptyList(), null, sessionId));
            }
            return createOkResponse(cohorts);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/info")
    @ApiOperation(value = "Get cohort information", position = 2)
    public Response infoSample(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") int cohortId) {
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
    public Response getSamples(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") int cohortId) {
        try {
            Cohort cohort = catalogManager.getCohort(cohortId, queryOptions, sessionId).first();
            queryOptions.put("id", cohort.getSamples());
            int studyId = catalogManager.getStudyIdByCohortId(cohortId);
            QueryResult<Sample> allSamples = catalogManager.getAllSamples(studyId, queryOptions, sessionId);
            allSamples.setId("getCohortSamples");
            return createOkResponse(allSamples);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private QueryResult<Cohort> createCohort(int studyId, String cohortName, Cohort.Type type, String cohortDescription, QueryOptions queryOptions) throws CatalogException {
        QueryResult<Sample> queryResult = catalogManager.getAllSamples(studyId, queryOptions, sessionId);
        List<Integer> sampleIds = new ArrayList<>(queryResult.getNumResults());
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
    public Response update(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") int cohortId,
                           @ApiParam(value = "", required = false) @QueryParam("name") String name,
                           @ApiParam(value = "", required = false) @QueryParam("creationDate") String creationDate,
                           @ApiParam(value = "", required = false) @QueryParam("description") String description,
                           @ApiParam(value = "Comma separated values of sampleIds. Will replace all existing sampleIds", required = true) @QueryParam("samples") String samples) {
        try {
            return createOkResponse(catalogManager.modifyCohort(cohortId, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }    }

    @POST
    @Path("/{cohortId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some user attributes using POST method", position = 4)
    public Response updateByPost(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") int cohortId,
                                 @ApiParam(value = "params", required = true) Map<String, Object> params) {
        try {
            return createOkResponse(catalogManager.modifyCohort(cohortId, new QueryOptions(params), sessionId));
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
            List<Integer> cohortIds = new ArrayList<>(split.length);
            for (String cohortIdStr : split) {
                cohortIds.add(Integer.parseInt(cohortIdStr));
            }
            if (calculate) {
                VariantStorage variantStorage = new VariantStorage(catalogManager);
                Integer outdirId = outdirIdStr == null ? null : catalogManager.getFileId(outdirIdStr);
                queryOptions.put(ToolManager.EXECUTE, false);
                queryOptions.add(AnalysisFileIndexer.LOG_LEVEL, logLevel);
                QueryResult<Job> jobQueryResult = variantStorage.calculateStats(outdirId, cohortIds, sessionId, queryOptions);
                return createOkResponse(jobQueryResult);
            } else if (delete) {
                List<QueryResult<Cohort>> results = new LinkedList<>();
                for (Integer cohortId : cohortIds) {
                    results.add(catalogManager.deleteCohort(cohortId, queryOptions, sessionId));
                }
                return createOkResponse(results);
            } else {
                int studyId = catalogManager.getStudyIdByCohortId(cohortIds.get(0));
                return createOkResponse(catalogManager.getAllCohorts(studyId,
                        new QueryOptions(CatalogSampleDBAdaptor.CohortFilterOption.id.toString(), cohortIdsCsv), sessionId).first());
            }

        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohortId}/delete")
    @ApiOperation(value = "Delete cohort.", position = 5)
    public Response deleteCohort(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") int cohortId) {
        try {
            return createOkResponse(catalogManager.deleteCohort(cohortId, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }    }

}
