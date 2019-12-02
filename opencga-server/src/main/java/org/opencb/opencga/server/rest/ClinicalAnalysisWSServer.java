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

import io.swagger.annotations.Api;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

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

//    @POST
//    @Path("/create")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "Create a new clinical analysis", position = 1, response = ClinicalAnalysis.class)
//    public Response create(
//            @ApiParam(value = Params.STUDY_DESCRIPTION) @QueryParam(Params.STUDY_PARAM)
//                    String studyId,
//            @ApiParam(name = "params", value = "JSON containing clinical analysis information", required = true)
//                    ClinicalAnalysisParameters params) {
//        try {
//            return createOkResponse(clinicalManager.create(studyId, params.toClinicalAnalysis(), queryOptions, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    @POST
//    @Path("/{clinicalAnalysis}/update")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "Update a clinical analysis", position = 1, response = ClinicalAnalysis.class)
//    public Response update(
//            @ApiParam(value = "Clinical analysis id") @PathParam(value = "clinicalAnalysis") String clinicalAnalysisStr,
//            @ApiParam(value = Params.STUDY_DESCRIPTION) @QueryParam(Params.STUDY_PARAM)
//                    String studyId,
//            @ApiParam(name = "params", value = "JSON containing clinical analysis information", required = true)
//                    ClinicalAnalysisParameters params) {
//        try {
//            ObjectMap parameters = new ObjectMap(getUpdateObjectMapper().writeValueAsString(params.toClinicalAnalysis()));
//
//            if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key())) {
//                Map<String, Object> actionMap = new HashMap<>();
//                actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key(), ParamUtils.UpdateAction.SET.name());
//                queryOptions.put(Constants.ACTIONS, actionMap);
//            }
//
//            // We remove the following parameters that are always going to appear because of Jackson
//            parameters.remove(ClinicalAnalysisDBAdaptor.QueryParams.UID.key());
//            parameters.remove(ClinicalAnalysisDBAdaptor.QueryParams.RELEASE.key());
//
//            return createOkResponse(clinicalManager.update(studyId, clinicalAnalysisStr, parameters, queryOptions, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    @POST
//    @Path("/{clinicalAnalysis}/interpretations/update")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "Update a clinical analysis", position = 1, response = ClinicalAnalysis.class)
//    public Response interpretationUpdate(
//            @ApiParam(value = "Clinical analysis id") @PathParam(value = "clinicalAnalysis") String clinicalAnalysisStr,
//            @ApiParam(value = Params.STUDY_DESCRIPTION) @QueryParam(Params.STUDY_PARAM)
//                    String studyId,
//            @ApiParam(value = "Action to be performed if the array of interpretations is being updated.", defaultValue = "ADD")
//                @QueryParam("action") ParamUtils.BasicUpdateAction interpretationAction,
//            @ApiParam(name = "params", value = "JSON containing clinical analysis information", required = true)
//                    ClinicalInterpretationParameters params) {
//        try {
//            if (interpretationAction == null) {
//                interpretationAction = ParamUtils.BasicUpdateAction.ADD;
//            }
//
//            Map<String, Object> actionMap = new HashMap<>();
//            actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key(), interpretationAction.name());
//            queryOptions.put(Constants.ACTIONS, actionMap);
//
//            ObjectMap parameters = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key(),
//                    Arrays.asList(getUpdateObjectMapper().writeValueAsString(params.toClinicalInterpretation())));
//
//            return createOkResponse(clinicalManager.update(studyId, clinicalAnalysisStr, parameters, queryOptions, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    @GET
//    @Path("/{clinicalAnalyses}/info")
//    @ApiOperation(value = "Clinical analysis info", position = 3, response = ClinicalAnalysis[].class)
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = Params.INCLUDE_DESCRIPTION,
//                    example = "name,attributes", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = Params.EXCLUDE_DESCRIPTION,
//                    example = "id,status", dataType = "string", paramType = "query")
//    })
//    public Response info(@ApiParam(value = "Comma separated list of clinical analysis IDs up to a maximum of 100")
//                             @PathParam(value = "clinicalAnalyses") String clinicalAnalysisStr,
//                         @ApiParam(value = Params.STUDY_DESCRIPTION)
//                         @QueryParam(Params.STUDY_PARAM) String studyId,
//                         @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
//                                 + "exception whenever one of the entries looked for cannot be shown for whichever reason",
//                                 defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
//        try {
//            query.remove(Params.STUDY_PARAM);
//            query.remove("clinicalAnalyses");
//
//            List<String> analysisList = getIdList(clinicalAnalysisStr);
//            List<DataResult<ClinicalAnalysis>> analysisResult = clinicalManager.get(studyId, analysisList, query, queryOptions, silent, sessionId);
//            return createOkResponse(analysisResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    @GET
//    @Path("/search")
//    @ApiOperation(value = "Clinical analysis search.", position = 12, response = ClinicalAnalysis[].class)
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = Params.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = Params.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.LIMIT, value = Params.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.SKIP, value = Params.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.COUNT, value = Params.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query")
//    })
//    public Response search(
//            @ApiParam(value = Params.STUDY_DESCRIPTION)
//            @QueryParam(Params.STUDY_PARAM) String studyId,
//            @ApiParam(value = "Clinical analysis type") @QueryParam("type") ClinicalAnalysis.Type type,
//            @ApiParam(value = "Priority") @QueryParam("priority") String priority,
//            @ApiParam(value = "Clinical analysis status") @QueryParam("status") String status,
//            @ApiParam(value = Params.CREATION_DATE_DESCRIPTION)
//                @QueryParam("creationDate") String creationDate,
//            @ApiParam(value = Params.MODIFICATION_DATE_DESCRIPTION)
//                @QueryParam("modificationDate") String modificationDate,
//            @ApiParam(value = "Due date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)") @QueryParam("dueDate") String dueDate,
//            @ApiParam(value = "Description") @QueryParam("description") String description,
//            @ApiParam(value = "Germline") @QueryParam("germline") String germline,
//            @ApiParam(value = "Somatic") @QueryParam("somatic") String somatic,
//            @ApiParam(value = "Family") @QueryParam("family") String family,
//            @ApiParam(value = "Proband") @QueryParam("proband") String proband,
//            @ApiParam(value = "Sample") @QueryParam("sample") String sample,
//            @ApiParam(value = "Release value") @QueryParam("release") String release,
//            @ApiParam(value = "Text attributes (Format: sex=male,age>20 ...)") @QueryParam("attributes") String attributes,
//            @ApiParam(value = "Numerical attributes (Format: sex=male,age>20 ...)") @QueryParam("nattributes") String nattributes) {
//        try {
//            query.remove(Params.STUDY_PARAM);
//
//            DataResult<ClinicalAnalysis> queryResult;
//            if (count) {
//                queryResult = clinicalManager.count(studyId, query, sessionId);
//            } else {
//                queryResult = clinicalManager.search(studyId, query, queryOptions, sessionId);
//            }
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    @GET
//    @Path("/groupBy")
//    @ApiOperation(value = "Group clinical analysis by several fields", position = 10, hidden = true,
//            notes = "Only group by categorical variables. Grouping by continuous variables might cause unexpected behaviour")
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = QueryOptions.COUNT, value = "Count the number of elements matching the group", dataType = "boolean",
//                    paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Maximum number of documents (groups) to be returned", dataType = "integer",
//                    paramType = "query", defaultValue = "50")
//    })
//    public Response groupBy(
//            @ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("") @QueryParam("fields") String fields,
//            @ApiParam(value = Params.STUDY_DESCRIPTION) @QueryParam(Params.STUDY_PARAM)
//                    String studyId,
//            @ApiParam(value = "Comma separated list of ids.") @QueryParam("id") String id,
//            @ApiParam(value = "DEPRECATED: Comma separated list of names.") @QueryParam("name") String name,
//            @ApiParam(value = "Clinical analysis type") @QueryParam("type") ClinicalAnalysis.Type type,
//            @ApiParam(value = "Clinical analysis status") @QueryParam("status") String status,
//            @ApiParam(value = "Germline") @QueryParam("germline") String germline,
//            @ApiParam(value = "Somatic") @QueryParam("somatic") String somatic,
//            @ApiParam(value = "Family") @QueryParam("family") String family,
//            @ApiParam(value = "Proband") @QueryParam("proband") String proband,
//            @ApiParam(value = "Sample") @QueryParam("sample") String sample,
//            @ApiParam(value = "Release value (Current release from the moment the families were first created)") @QueryParam("release") String release) {
//        try {
//            query.remove(Params.STUDY_PARAM);
//            query.remove("fields");
//
//            DataResult result = clinicalManager.groupBy(studyId, query, fields, queryOptions, sessionId);
//            return createOkResponse(result);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    @GET
//    @Path("/{clinicalAnalyses}/acl")
//    @ApiOperation(value = "Returns the acl of the clinical analyses. If member is provided, it will only return the acl for the member.",
//            position = 18)
//    public Response getAcls(
//            @ApiParam(value = "Comma separated list of clinical analysis IDs or names up to a maximum of 100", required = true)
//                @PathParam("clinicalAnalyses") String clinicalAnalysis,
//            @ApiParam(value = Params.STUDY_DESCRIPTION) @QueryParam(Params.STUDY_PARAM) String studyId,
//            @ApiParam(value = "User or group id") @QueryParam("member") String member,
//            @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
//                    + "exception whenever one of the entries looked for cannot be shown for whichever reason",
//                    defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
//        try {
//            List<String> idList = getIdList(clinicalAnalysis);
//            return createOkResponse(clinicalManager.getAcls(studyId, idList, member, silent, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    public static class ClinicalAnalysisAcl extends AclParams {
//        public String clinicalAnalysis;
//    }
//
//    @POST
//    @Path("/acl/{members}/update")
//    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
//    public Response updateAcl(
//            @ApiParam(value = Params.STUDY_DESCRIPTION) @QueryParam(Params.STUDY_PARAM) String studyId,
//            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
//            @ApiParam(value = "JSON containing the parameters to add ACLs", required = true) ClinicalAnalysisAcl params) {
//        try {
//            params = ObjectUtils.defaultIfNull(params, new ClinicalAnalysisAcl());
//            AclParams clinicalAclParams = new AclParams(params.getPermissions(), params.getAction());
//            List<String> idList = getIdList(params.clinicalAnalysis);
//            return createOkResponse(clinicalManager.updateAcl(studyId, idList, memberId, clinicalAclParams, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    private static class SampleParams {
//        public String id;
//    }
//
//    private static class ProbandParam {
//        public String id;
//        public List<SampleParams> samples;
//    }
//
//    private static class FamilyParam {
//        public String id;
//        public List<ProbandParam> members;
//    }
//
//    private static class ClinicalInterpretationParameters {
//        public String id;
//        public String description;
//        public String clinicalAnalysisId;
//        public List<DiseasePanel> panels;
//        public Software software;
//        public Analyst analyst;
//        public List<Software> dependencies;
//        public Map<String, Object> filters;
//        public String creationDate;
//        public List<ReportedVariant> primaryFindings;
//        public List<ReportedLowCoverage> reportedLowCoverages;
//        public List<Comment> comments;
//        public Map<String, Object> attributes;
//
//        public Interpretation toClinicalInterpretation() {
//            return new Interpretation(id, description, clinicalAnalysisId, panels, software, analyst, dependencies, filters, creationDate,
//                    primaryFindings, reportedLowCoverages, comments, attributes);
//        }
//    }
//
//    private static class ClinicalAnalysisParameters {
//        public String id;
//        @Deprecated
//        public String name;
//        public String description;
//        public ClinicalAnalysis.Type type;
//
//        public Disorder disorder;
//
//        public Map<String, List<String>> files;
//
//        public ProbandParam proband;
//        public FamilyParam family;
//        public ClinicalAnalysis.ClinicalStatus status;
//        public List<ClinicalInterpretationParameters> interpretations;
//
//        public String dueDate;
//        public List<Comment> comments;
//        public ClinicalAnalysis.Priority priority;
//
//        public Map<String, Object> attributes;
//
//        public ClinicalAnalysis toClinicalAnalysis() {
//
//            Individual individual = null;
//            if (proband != null) {
//                individual = new Individual().setId(proband.id);
//                if (proband.samples != null) {
//                    List<Sample> sampleList = proband.samples.stream()
//                            .map(sample -> new Sample().setId(sample.id))
//                            .collect(Collectors.toList());
//                    individual.setSamples(sampleList);
//                }
//            }
//
//            Map<String, List<File>> fileMap = new HashMap<>();
//            if (files != null) {
//                for (Map.Entry<String, List<String>> entry : files.entrySet()) {
//                    List<File> fileList = entry.getValue().stream().map(fileId -> new File().setPath(fileId)).collect(Collectors.toList());
//                    fileMap.put(entry.getKey(), fileList);
//                }
//            }
//
//            Family f = null;
//            if (family != null) {
//                f = new Family().setId(family.id);
//                if (family.members != null) {
//                    List<Individual> members = new ArrayList<>(family.members.size());
//                    for (ProbandParam member : family.members) {
//                        Individual auxIndividual = new Individual().setId(member.id);
//                        if (member.samples != null) {
//                            List<Sample> samples = member.samples.stream().map(s -> new Sample().setId(s.id)).collect(Collectors.toList());
//                            auxIndividual.setSamples(samples);
//                        }
//                        members.add(auxIndividual);
//                    }
//                    f.setMembers(members);
//                }
//            }
//
//            List<Interpretation> interpretationList =
//                    interpretations != null
//                            ? interpretations.stream()
//                            .map(ClinicalInterpretationParameters::toClinicalInterpretation).collect(Collectors.toList())
//                            : new ArrayList<>();
//            String clinicalId = StringUtils.isEmpty(id) ? name : id;
//            return new ClinicalAnalysis(clinicalId, description, type, disorder, fileMap, individual, f,
//                    interpretationList, priority, null, dueDate, comments, status, 1, attributes).setName(name);
//        }
//    }

}
