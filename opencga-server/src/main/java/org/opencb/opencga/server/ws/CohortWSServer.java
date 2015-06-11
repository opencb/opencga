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
import java.util.stream.Collector;
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
    @ApiOperation(value = "Create a cohort", position = 1)
    public Response createCohort(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                 @ApiParam(value = "name", required = true) @QueryParam("name") String cohortName,
                                 @ApiParam(value = "variableSetId", required = true) @QueryParam("variableSetId") int variableSetId,
                                 @ApiParam(value = "description", required = false) @QueryParam("description") String cohortDescription,
                                 @ApiParam(value = "sampleIds", required = false) @QueryParam("sampleIds") String sampleIdsStr,
                                 @ApiParam(value = "variable", required = false) @QueryParam("variable") String variableName) {
        try {
            //QueryOptions queryOptions = getAllQueryOptions();
            List<Cohort> cohorts = new LinkedList<>();
            if (variableName != null && !variableName.isEmpty() && sampleIdsStr != null && !sampleIdsStr.isEmpty()) {
                return createErrorResponse("", "Can only create a cohort given list of sampleIds or a categorical variable name");
            }

            int studyId = catalogManager.getStudyId(studyIdStr);
            if (sampleIdsStr != null && !sampleIdsStr.isEmpty()) {
                QueryOptions samplesQuery = new QueryOptions("include", "projects.studies.samples.id");
                samplesQuery.add("id", sampleIdsStr);
                cohorts.add(createCohort(studyId, cohortName, cohortDescription, samplesQuery).first());
            } else {
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
                    cohorts.add(createCohort(studyId, s, cohortDescription, samplesQuery).first());
                }
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

    private QueryResult<Cohort> createCohort(int studyId, String cohortName, String cohortDescription, QueryOptions queryOptions) throws CatalogException {
        QueryResult<Sample> queryResult = catalogManager.getAllSamples(studyId, queryOptions, sessionId);
        List<Integer> sampleIds = new ArrayList<>(queryResult.getNumResults());
        sampleIds.addAll(queryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList()));
        return catalogManager.createCohort(studyId, cohortName, cohortDescription, sampleIds, null, sessionId);
    }

//
//    @GET
//    @Path("/create")
//    @Produces("application/json")
//    @ApiOperation(value = "Create a cohort")
//    public Response createCohort(
//            @ApiParam(value = "studyId", required = true) @QueryParam("studyId") int studyId,
//            @ApiParam(value = "cohortName", required = true) @QueryParam("cohortName") String cohortName,
//            @ApiParam(value = "cohortDescription", required = false) @QueryParam("cohortDescription") String cohortDescription) {
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
    public Response update(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortId) throws IOException {
        return createErrorResponse("update - GET", "PENDING");
    }

    @POST
    @Path("/{cohortId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some user attributes using POST method", position = 4)
    public Response updateByPost(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") String cohortId,
                                 @ApiParam(value = "params", required = true) Map<String, Object> params) {
        return createErrorResponse("update - POST", "PENDING");
    }

    @GET
    @Path("/{cohortId}/delete")
    @ApiOperation(value = "Delete cohort. PENDING", position = 5)
    public Response deleteCohort(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") int cohortId) {
        return createErrorResponse("delete", "PENDING");
    }

}
