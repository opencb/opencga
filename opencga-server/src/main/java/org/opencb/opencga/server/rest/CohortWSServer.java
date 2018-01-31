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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.CohortManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.server.WebServiceException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;

/**
 * Created by jacobo on 15/12/14.
 */
@Path("/{apiVersion}/cohorts")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Cohorts", position = 9, description = "Methods for working with 'cohorts' endpoint")
public class CohortWSServer extends OpenCGAWSServer {

    private CohortManager cohortManager;

    public CohortWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        cohortManager = catalogManager.getCohortManager();
    }

    private Response createCohort(String studyStr, String cohortName, Study.Type type, String variableSetId, String cohortDescription,
                                  String sampleIdsStr, List<AnnotationSet> annotationSetList, String variableName) {
        try {
            List<QueryResult<Cohort>> cohorts = new LinkedList<>();
            if (variableName != null && !variableName.isEmpty() && sampleIdsStr != null && !sampleIdsStr.isEmpty()) {
                return createErrorResponse("", "Can only create a cohort given list of sampleIds or a categorical "
                        + "variable name");
            }

            String userId = catalogManager.getUserManager().getUserId(sessionId);
            long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
            if (StringUtils.isNotEmpty(sampleIdsStr)) {
                List<String> idList = getIdList(sampleIdsStr);
                AbstractManager.MyResourceIds samples = catalogManager.getSampleManager().getIds(idList, Long.toString(studyId), sessionId);
                List<Sample> sampleList = new ArrayList<>(samples.getResourceIds().size());
                for (Long sampleId : samples.getResourceIds()) {
                    sampleList.add(new Sample().setId(sampleId));
                }
                QueryResult<Cohort> cohortQueryResult = catalogManager.getCohortManager().create(studyId, cohortName, type,
                        cohortDescription, sampleList, annotationSetList, null, sessionId);
                cohorts.add(cohortQueryResult);
            } else if (StringUtils.isNotEmpty(variableSetId)) {
                VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(Long.toString(studyId), variableSetId, null, sessionId).first();
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
                            .append("variableSetId", variableSet.getId());

                    cohorts.add(createCohort(studyId, cohortName + "_" + s, type, cohortDescription, annotationSetList, samplesQuery, samplesQOptions));
                }
            } else {
                //Create empty cohort
                cohorts.add(catalogManager.getCohortManager().create(studyId, cohortName, type, cohortDescription,
                        Collections.emptyList(), annotationSetList, null, sessionId));
            }
            return createOkResponse(cohorts);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create a cohort", position = 1, notes = "A cohort can be created by providing a list of SampleIds, " +
            "or providing a categorical variable (both variableSet and variable). " +
            "If none of this is given, an empty cohort will be created.", response = Cohort.class)
    public Response createCohort(
            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Variable set id or name", hidden = true) @QueryParam("variableSetId")
                    String variableSetId,
            @ApiParam(value = "Variable set id or name") @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Variable name") @QueryParam("variable") String variableName,
            @ApiParam(value = "JSON containing cohort information", required = true) CohortParameters params) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            if (StringUtils.isNotEmpty(variableSetId)) {
                variableSet = variableSetId;
            }

            List<AnnotationSet> annotationSetList = new ArrayList<>();
            if (params.annotationSets != null) {
                for (CommonModels.AnnotationSetParams annotationSet : params.annotationSets) {
                    if (annotationSet != null) {
                        annotationSetList.add(annotationSet.toAnnotationSet(studyStr, catalogManager.getStudyManager(), sessionId));
                    }
                }
            }

            return createCohort(studyStr, params.name, params.type, variableSet, params.description, params.samples,
                    annotationSetList, variableName);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohorts}/info")
    @ApiOperation(value = "Get cohort information", position = 2, response = Cohort.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response infoSample(@ApiParam(value = "Comma separated list of cohort names or ids up to a maximum of 100", required = true) @PathParam("cohorts")
                                       String cohortsStr,
                               @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                               @QueryParam("study") String studyStr,
                               @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            List<String> cohortList = getIdList(cohortsStr);
            List<QueryResult<Cohort>> cohortQueryResult = cohortManager.get(studyStr, cohortList, query, queryOptions, silent, sessionId);
            return createOkResponse(cohortQueryResult);
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
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", defaultValue = "false", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = Constants.FLATTENED_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response searchCohorts(@ApiParam(value = "Study [[user@]project:]study where study and project can be either "
            + "the id or alias") @QueryParam("study") String studyStr,
                                  @ApiParam(value = "Name of the cohort") @QueryParam("name") String name,
                                  @ApiParam(value = "Cohort type") @QueryParam("type") Study.Type type,
                                  @ApiParam(value = "Status") @QueryParam("status") String status,
                                  @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,
                                  @ApiParam(value = "Sample list") @QueryParam("samples") String samplesStr,
                                  @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount,
                                  @ApiParam(value = "Release value") @QueryParam("release") String release) {
        try {
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);
            QueryResult<Cohort> queryResult;
            if (count) {
                queryResult = catalogManager.getCohortManager().count(studyStr, query, sessionId);
            } else {
                queryResult = catalogManager.getCohortManager().search(studyStr, query, queryOptions, sessionId);
            }
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohort}/samples")
    @ApiOperation(value = "Get samples from cohort [WARNING]", position = 3, response = Sample[].class,
            notes = "The usage of this webservice is discouraged. /{cohorts}/info is expected to be used with &include=samples query"
                    + " parameter to approximately simulate this same behaviour.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response getSamples(@ApiParam(value = "Cohort id or name", required = true) @PathParam("cohort") String cohortStr,
                               @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                               @QueryParam("study") String studyStr) {
        try {
            return createOkResponse(cohortManager.getSamples(studyStr, cohortStr, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private QueryResult<Cohort> createCohort(long studyId, String cohortName, Study.Type type, String cohortDescription, List
            <AnnotationSet> annotationSetList, Query query, QueryOptions queryOptions) throws CatalogException {
        //TODO CHANGE THIS for can insert the name also id(number)
        QueryResult<Sample> queryResult = catalogManager.getSampleManager().get(studyId, query, queryOptions, sessionId);
        //TODO FOR THIS. Its possible change the param query to a String
        //List<QueryResult<Sample>> queryResults = new LinkedList<>();
        //List<Long> sampleIds = catalogManager.getSampleIds(query.get("id").toString(), sessionId);
        return catalogManager.getCohortManager().create(studyId, cohortName, type, cohortDescription, queryResult.getResult(), annotationSetList,
                null, sessionId);
    }

    @POST
    @Path("/{cohort}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some cohort attributes", position = 4)
    public Response updateByPost(@ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortStr,
                                 @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                 @QueryParam("study") String studyStr,
                                 @ApiParam(value = "params", required = true) Map<String, Object> params) {
        try {
            return createOkResponse(catalogManager.getCohortManager().update(studyStr, cohortStr, new ObjectMap(params), queryOptions,
                    sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohorts}/delete")
    @ApiOperation(value = "Delete cohort. [WARNING]", position = 5,
            notes = "Usage of this webservice might lead to unexpected behaviour and therefore is discouraged to use. Deletes are " +
                    "planned to be fully implemented and tested in version 1.4.0")
    public Response deleteCohort(@ApiParam(value = "Comma separated list of cohort Ids", required = true) @PathParam("cohorts") String cohortsStr,
                                 @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                 @QueryParam("study") String studyStr,
                                 @QueryParam("silent") boolean silent) {
        try {
//            long cohortId = catalogManager.getCohortId(cohortStr, sessionId);
            List<QueryResult<Cohort>> delete = cohortManager.delete(studyStr, cohortsStr, queryOptions, sessionId);
            return createOkResponse(delete);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohort}/annotationsets/search")
    @ApiOperation(value = "Search annotation sets", position = 11)
    public Response searchAnnotationSetGET(
            @ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Variable set id or name", required = true) @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Annotation, e.g: key1=value(,key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            AbstractManager.MyResourceId resourceId = cohortManager.getId(cohortStr, studyStr, sessionId);

            Query query = new Query()
                    .append(CohortDBAdaptor.QueryParams.STUDY_ID.key(), resourceId.getStudyId())
                    .append(CohortDBAdaptor.QueryParams.ID.key(), resourceId.getResourceId())
                    .append(Constants.FLATTENED_ANNOTATIONS, asMap);

            String variableSetId = String.valueOf(catalogManager.getStudyManager()
                    .getVariableSetId(variableSet, String.valueOf(resourceId.getStudyId()), sessionId).getResourceId());

            if (StringUtils.isEmpty(annotation)) {
                annotation = Constants.VARIABLE_SET + "=" + variableSetId;
            } else {
                annotation += ";" + Constants.VARIABLE_SET + "=" + variableSetId;
            }
            query.append(Constants.ANNOTATION, annotation);

            QueryResult<Cohort> search = cohortManager.search(String.valueOf(resourceId.getStudyId()), query, new QueryOptions(),
                    sessionId);
            if (search.getNumResults() == 1) {
                return createOkResponse(new QueryResult<>("Search", search.getDbTime(), search.first().getAnnotationSets().size(),
                        search.first().getAnnotationSets().size(), search.getWarningMsg(), search.getErrorMsg(),
                        search.first().getAnnotationSets()));
            } else {
                return createOkResponse(search);
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohorts}/annotationsets")
    @ApiOperation(value = "Return all the annotation sets of the cohort", position = 12)
    public Response getAnnotationSet(
            @ApiParam(value = "Comma separated list of cohort Ids up to a maximum of 100", required = true) @PathParam("cohorts") String cohortsStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap,
            @ApiParam(value = "Annotation set name. If provided, only chosen annotation set will be shown") @QueryParam("name") String annotationsetName,
            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false") @QueryParam("silent") boolean silent) throws WebServiceException {
        try {
            AbstractManager.MyResourceIds resourceIds = cohortManager.getIds(cohortsStr, studyStr, sessionId);

            Query query = new Query()
                    .append(CohortDBAdaptor.QueryParams.STUDY_ID.key(), resourceIds.getStudyId())
                    .append(CohortDBAdaptor.QueryParams.ID.key(), resourceIds.getResourceIds())
                    .append(Constants.FLATTENED_ANNOTATIONS, asMap);

            if (StringUtils.isNotEmpty(annotationsetName)) {
                query.append(Constants.ANNOTATION, Constants.ANNOTATION_SET_NAME + "=" + annotationsetName);
            }

            QueryResult<Cohort> search = cohortManager.search(String.valueOf(resourceIds.getStudyId()), query, new QueryOptions(),
                    sessionId);
            if (search.getNumResults() == 1) {
                return createOkResponse(new QueryResult<>("List annotationSets", search.getDbTime(),
                        search.first().getAnnotationSets().size(), search.first().getAnnotationSets().size(), search.getWarningMsg(),
                        search.getErrorMsg(), search.first().getAnnotationSets()));
            } else {
                return createOkResponse(search);
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
    @ApiOperation(value = "Create an annotation set for the cohort", position = 13)
    public Response annotateSamplePOST(
            @ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Variable set id or name", hidden = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = "Variable set id or name", required = true) @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "JSON containing the annotation set name and the array of annotations. The name should be unique for the "
                    + "cohort", required = true) AnnotationsetParameters params) {
        try {
            if (StringUtils.isNotEmpty(variableSetId)) {
                variableSet = variableSetId;
            }
            QueryResult<AnnotationSet> queryResult = cohortManager.createAnnotationSet(cohortStr, studyStr, variableSet,
                    params.name, params.annotations, Collections.emptyMap(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohort}/annotationsets/{annotationsetName}/delete")
    @ApiOperation(value = "Delete the annotation set or the annotations within the annotation set", position = 14)
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
    @ApiOperation(value = "Update the annotations", position = 15)
    public Response updateAnnotationGET(
            @ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName") String annotationsetName,
            @ApiParam(value = "JSON containing key:value annotations to update", required = true) Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult = cohortManager.updateAnnotationSet(cohortIdStr, studyStr, annotationsetName,
                    annotations, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group cohorts by several fields", position = 24,
            notes = "Only group by categorical variables. Grouping by continuous variables might cause unexpected behaviour")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "count", value = "Count the number of elements matching the group", dataType = "boolean",
                    paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Maximum number of documents (groups) to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "50")
    })
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
                            @QueryParam("fields") String fields,
                            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @DefaultValue("") @QueryParam("studyId")
                                    String studyIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr,
                            @ApiParam(value = "Comma separated list of names.", required = false) @DefaultValue("") @QueryParam("name")
                                    String names,
                            @ApiParam(value = "Comma separated Type values.", required = false) @DefaultValue("") @QueryParam("type")
                                    String type,
                            @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") String status,
                            @ApiParam(value = "creationDate", required = false) @DefaultValue("") @QueryParam("creationDate")
                                    String creationDate,
                            @ApiParam(value = "Comma separated sampleIds", required = false) @DefaultValue("") @QueryParam("sampleIds")
                                    String sampleIds) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            QueryResult result = cohortManager.groupBy(studyStr, query, fields, queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohorts}/acl")
    @ApiOperation(value = "Return the acl of the cohort. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(
            @ApiParam(value = "Comma separated list of cohort ids up to a maximum of 100", required = true) @PathParam("cohorts") String cohortIdsStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "User or group id") @QueryParam("member") String member,
            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            List<String> idList = getIdList(cohortIdsStr);
            return createOkResponse(cohortManager.getAcls(studyStr, idList, member, silent, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{cohort}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member [DEPRECATED]", position = 21, hidden = true,
            notes = "DEPRECATED: The usage of this webservice is discouraged. A different entrypoint /acl/{members}/update has been added "
                    + "to also support changing permissions using queries.")
    public Response updateAcl(
            @ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value = "JSON containing one of the keys 'add', 'set' or 'remove'", required = true) StudyWSServer.MemberAclUpdateOld params) {
        try {
            AclParams aclParams = getAclParams(params.add, params.remove, params.set);
            List<String> idList = getIdList(cohortIdStr);
            return createOkResponse(cohortManager.updateAcl(studyStr, idList, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class CohortAcl extends AclParams {
        public String cohort;
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = "JSON containing the parameters to add ACLs", required = true) CohortAcl params) {
        try {
            AclParams aclParams = new AclParams(params.getPermissions(), params.getAction());
            List<String> idList = getIdList(params.cohort);
            return createOkResponse(cohortManager.updateAcl(studyStr, idList, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    protected static class CohortParameters {
        public String name;
        public Study.Type type;
        public String description;
        public String samples;
        public List<CommonModels.AnnotationSetParams> annotationSets;
        public Map<String, Object> attributes;
    }

}
