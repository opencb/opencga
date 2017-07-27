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
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Map;

/**
 * Created by pfurio on 05/06/17.
 */
@Path("/{version}/clinical")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Clinical Analysis (BETA)", position = 9, description = "Methods for working with 'clinical analysis' endpoint")

public class ClinicalAnalysisWSServer extends OpenCGAWSServer {

    private final ClinicalAnalysisManager clinicalManager;

    public ClinicalAnalysisWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        clinicalManager = catalogManager.getClinicalAnalysisManager();
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new clinical analysis", position = 1, response = ClinicalAnalysis.class)
    public Response create(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(name = "params", value="JSON containing clinical analysis information", required = true)
                ClinicalAnalysisParameters params) {
        try {
            return createOkResponse(clinicalManager.create(studyStr, params.toClinicalAnalysis(), queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{clinicalAnalysis}/info")
    @ApiOperation(value = "Clinical analysis info", position = 3, response = ClinicalAnalysis[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query")
    })
    public Response info(@ApiParam(value="Comma separated list of clinical analysis ids") @PathParam(value = "clinicalAnalysis")
                                     String clinicalAnalysisStr,
                         @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                         @QueryParam("study") String studyStr) {
        try {
            return createOkResponse(clinicalManager.get(studyStr, clinicalAnalysisStr, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Clinical analysis search.", position = 12, response = ClinicalAnalysis[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = "Study [[user@]project:]{study} where study and project can be either the id or alias.")
                @QueryParam("study") String studyStr,
            @ApiParam(value = "Clinical analysis type") @QueryParam("type") ClinicalAnalysis.Type type,
            @ApiParam(value = "Clinical analysis status") @QueryParam("status") String status,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Description") @QueryParam("description") String description,
            @ApiParam(value = "Family") @QueryParam("family") String family,
            @ApiParam(value = "Proband") @QueryParam("proband") String proband,
            @ApiParam(value = "Sample") @QueryParam("sample") String sample,
            @ApiParam(value = "Release value") @QueryParam("release") String release,
            @ApiParam(value = "Text attributes (Format: sex=male,age>20 ...)") @QueryParam("attributes") String attributes,
            @ApiParam(value = "Numerical attributes (Format: sex=male,age>20 ...)") @QueryParam("nattributes") String nattributes) {
        try {
            QueryResult<ClinicalAnalysis> queryResult;
            if (count) {
                queryResult = clinicalManager.count(studyStr, query, sessionId);
            } else {
                queryResult = clinicalManager.search(studyStr, query, queryOptions, sessionId);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private static class ClinicalAnalysisParameters {
        public String name;
        public String description;
        public ClinicalAnalysis.Type type;

        public String family;
        public String proband;
        public String sample;

        public Map<String, Object> attributes;

        public ClinicalAnalysis toClinicalAnalysis() {
            return new ClinicalAnalysis(-1, name, description, type, new Family().setName(family), new Individual().setName(proband),
                    new Sample().setName(sample), null, null, 1, attributes);
        }
    }

}
