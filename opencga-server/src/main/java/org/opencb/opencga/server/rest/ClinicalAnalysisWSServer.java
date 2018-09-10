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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.OntologyTerm;
import org.opencb.biodata.models.commons.Software;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.clinical.Comment;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.clinical.ReportedVariant;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 05/06/17.
 */
@Path("/{apiVersion}/clinical")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Clinical Analysis", position = 9, description = "Methods for working with 'clinical analysis' endpoint")

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
            @ApiParam(name = "params", value = "JSON containing clinical analysis information", required = true)
                    ClinicalAnalysisParameters params) {
        try {
            return createOkResponse(clinicalManager.create(studyStr, params.toClinicalAnalysis(), queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{clinicalAnalysis}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update a clinical analysis", position = 1, response = ClinicalAnalysis.class)
    public Response update(
            @ApiParam(value = "Clinical analysis id") @PathParam(value = "clinicalAnalysis") String clinicalAnalysisStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(name = "params", value = "JSON containing clinical analysis information", required = true)
                    ClinicalAnalysisParameters params) {
        try {
            ObjectMap parameters = new ObjectMap(jsonObjectMapper.writeValueAsString(params.toClinicalAnalysis()));

            if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key())) {
                Map<String, Object> actionMap = new HashMap<>();
                actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key(), ParamUtils.UpdateAction.SET.name());
                queryOptions.put(Constants.ACTIONS, actionMap);
            }

            // We remove the following parameters that are always going to appear because of Jackson
            parameters.remove(ClinicalAnalysisDBAdaptor.QueryParams.UID.key());
            parameters.remove(ClinicalAnalysisDBAdaptor.QueryParams.RELEASE.key());

            return createOkResponse(clinicalManager.update(studyStr, clinicalAnalysisStr, parameters, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{clinicalAnalysis}/interpretations/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update a clinical analysis", position = 1, response = ClinicalAnalysis.class)
    public Response interpretationUpdate(
            @ApiParam(value = "Clinical analysis id") @PathParam(value = "clinicalAnalysis") String clinicalAnalysisStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Action to be performed if the array of interpretations is being updated.", defaultValue = "ADD")
            @QueryParam("interpretationAction") ParamUtils.BasicUpdateAction interpretationAction,
            @ApiParam(name = "params", value = "JSON containing clinical analysis information", required = true)
                    ClinicalInterpretationParameters params) {
        try {
            if (interpretationAction == null) {
                interpretationAction = ParamUtils.BasicUpdateAction.ADD;
            }

            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key(), interpretationAction.name());
            queryOptions.put(Constants.ACTIONS, actionMap);

            ObjectMap parameters = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key(),
                    Arrays.asList(jsonObjectMapper.writeValueAsString(params.toClinicalInterpretation())));

            return createOkResponse(clinicalManager.update(studyStr, clinicalAnalysisStr, parameters, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{clinicalAnalyses}/info")
    @ApiOperation(value = "Clinical analysis info", position = 3, response = ClinicalAnalysis[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query")
    })
    public Response info(@ApiParam(value = "Comma separated list of clinical analysis IDs up to a maximum of 100")
                             @PathParam(value = "clinicalAnalyses") String clinicalAnalysisStr,
                         @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                         @QueryParam("study") String studyStr,
                         @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            List<String> analysisList = getIdList(clinicalAnalysisStr);
            List<QueryResult<ClinicalAnalysis>> analysisResult = clinicalManager.get(studyStr, analysisList, query, queryOptions, silent, sessionId);
            return createOkResponse(analysisResult);
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
            @ApiImplicitParam(name = "count", value = "Total number of results", defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = "Study [[user@]project:]{study} where study and project can be either the id or alias.")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Clinical analysis type") @QueryParam("type") ClinicalAnalysis.Type type,
            @ApiParam(value = "Clinical analysis status") @QueryParam("status") String status,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Description") @QueryParam("description") String description,
            @ApiParam(value = "Germline") @QueryParam("germline") String germline,
            @ApiParam(value = "Somatic") @QueryParam("somatic") String somatic,
            @ApiParam(value = "Family") @QueryParam("family") String family,
            @ApiParam(value = "Subject") @QueryParam("subject") String subject,
            @ApiParam(value = "Sample") @QueryParam("sample") String sample,
            @ApiParam(value = "Release value") @QueryParam("release") String release,
            @ApiParam(value = "Text attributes (Format: sex=male,age>20 ...)") @QueryParam("attributes") String attributes,
            @ApiParam(value = "Numerical attributes (Format: sex=male,age>20 ...)") @QueryParam("nattributes") String nattributes) {
        try {
            query.remove("study");

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

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group clinical analysis by several fields", position = 10, hidden = true,
            notes = "Only group by categorical variables. Grouping by continuous variables might cause unexpected behaviour")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "count", value = "Count the number of elements matching the group", dataType = "boolean",
                    paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Maximum number of documents (groups) to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "50")
    })
    public Response groupBy(
            @ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("") @QueryParam("fields") String fields,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Comma separated list of ids.") @QueryParam("id") String id,
            @ApiParam(value = "DEPRECATED: Comma separated list of names.") @QueryParam("name") String name,
            @ApiParam(value = "Clinical analysis type") @QueryParam("type") ClinicalAnalysis.Type type,
            @ApiParam(value = "Clinical analysis status") @QueryParam("status") String status,
            @ApiParam(value = "Germline") @QueryParam("germline") String germline,
            @ApiParam(value = "Somatic") @QueryParam("somatic") String somatic,
            @ApiParam(value = "Family") @QueryParam("family") String family,
            @ApiParam(value = "Subject") @QueryParam("subject") String subject,
            @ApiParam(value = "Sample") @QueryParam("sample") String sample,
            @ApiParam(value = "Release value (Current release from the moment the families were first created)") @QueryParam("release") String release) {
        try {
            query.remove("study");
            query.remove("fields");

            QueryResult result = clinicalManager.groupBy(studyStr, query, fields, queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private static class SampleParams {
        public String id;
    }

    private static class SubjectParam {
        public String id;
        public List<SampleParams> samples;
    }

    private static class ClinicalInterpretationParameters {
        public String id;
        public String description;
        public String clinicalAnalysisId;
        public List<DiseasePanel> panels;
        public Software software;
        public Analyst analyst;
        public List<Software> dependencies;
        public Map<String, Object> filters;
        public String creationDate;
        public List<ReportedVariant> reportedVariants;
        public List<Comment> comments;
        public Map<String, Object> attributes;
        public Interpretation toClinicalInterpretation() {
            return new Interpretation(id, description, clinicalAnalysisId, panels, software, analyst, dependencies, filters, creationDate,
                    reportedVariants, comments, attributes);
        }
    }

    private static class ClinicalAnalysisParameters {
        public String id;
        @Deprecated
        public String name;
        public String description;
        public ClinicalAnalysis.Type type;

        public OntologyTerm disease;

        public String germline;
        public String somatic;

        public SubjectParam subject;
        public String family;
        public List<ClinicalInterpretationParameters> interpretations;

        public Map<String, Object> attributes;

        public ClinicalAnalysis toClinicalAnalysis() {

            Individual individual = null;
            if (subject != null) {
                individual = new Individual().setId(subject.id);
                if (subject.samples != null) {
                    List<Sample> sampleList = subject.samples.stream()
                            .map(sample -> new Sample().setId(sample.id))
                            .collect(Collectors.toList());
                    individual.setSamples(sampleList);
                }
            }

            File germlineFile = StringUtils.isNotEmpty(germline) ? new File().setName(germline) : null;
            File somaticFile = StringUtils.isNotEmpty(somatic) ? new File().setName(somatic) : null;

            Family f = null;
            if (StringUtils.isNotEmpty(family)) {
                f = new Family().setId(family);
            }

            List<Interpretation> interpretationList =
                    interpretations != null
                            ? interpretations.stream()
                            .map(ClinicalInterpretationParameters::toClinicalInterpretation).collect(Collectors.toList())
                            : new ArrayList<>();
            String clinicalId = StringUtils.isEmpty(id) ? name : id;
            return new ClinicalAnalysis(clinicalId, description, type, disease, germlineFile, somaticFile, individual, f,
                    interpretationList, null, null, 1, attributes).setName(name);
        }
    }

}
