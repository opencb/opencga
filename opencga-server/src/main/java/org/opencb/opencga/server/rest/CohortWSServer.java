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
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CohortManager;
import org.opencb.opencga.catalog.models.update.CohortUpdateParams;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.server.WebServiceException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jacobo on 15/12/14.
 */
@Path("/{apiVersion}/cohorts")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Cohorts", position = 9, description = "Methods for working with 'cohorts' endpoint")
public class CohortWSServer extends OpenCGAWSServer {

    private CohortManager cohortManager;

    public CohortWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        cohortManager = catalogManager.getCohortManager();
    }

    private Response createCohort(String studyStr, String cohortId, String cohortName, Study.Type type, String variableSetId,
                                  String cohortDescription, List<String> sampleIdList, List<AnnotationSet> annotationSetList,
                                  String variableName) {
        try {
            List<DataResult<Cohort>> cohorts = new LinkedList<>();
            if (StringUtils.isNotEmpty(variableName) && ListUtils.isNotEmpty(sampleIdList)) {
                return createErrorResponse("", "Can only create a cohort given list of sampleIds or a categorical variable name");
            }

            if (ListUtils.isNotEmpty(sampleIdList)) {
                List<DataResult<Sample>> queryResults = catalogManager.getSampleManager().get(studyStr, sampleIdList, null, sessionId);
                List<Sample> sampleList = queryResults.stream().map(DataResult::first).collect(Collectors.toList());
                Cohort cohort = new Cohort(cohortId, type, "", cohortDescription, sampleList, annotationSetList, -1, null)
                        .setName(cohortName);
                DataResult<Cohort> cohortQueryResult = catalogManager.getCohortManager().create(studyStr, cohort, null, sessionId);
                cohorts.add(cohortQueryResult);
            } else if (StringUtils.isNotEmpty(variableSetId)) {
                VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(studyStr, variableSetId, null, sessionId).first();
                Variable variable = null;
                for (Variable v : variableSet.getVariables()) {
                    if (v.getId().equals(variableName)) {
                        variable = v;
                        break;
                    }
                }
                if (variable == null) {
                    return createErrorResponse("", "Variable " + variableName + " does not exist in variableSet " + variableSet.getId());
                }
                if (variable.getType() != Variable.VariableType.CATEGORICAL) {
                    return createErrorResponse("", "Can only create cohorts by variable, when is a categorical variable");
                }
                for (String s : variable.getAllowedValues()) {
                    QueryOptions samplesQOptions = new QueryOptions("include", "projects.studies.samples.id");
                    Query samplesQuery = new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key() + "." + variableName, s)
                            .append("variableSetId", variableSet.getUid());

                    cohorts.add(createCohort(studyStr, cohortName + "_" + s, type, cohortDescription, annotationSetList, samplesQuery,
                            samplesQOptions));
                }
            } else {
                //Create empty cohort
                Cohort cohort = new Cohort(cohortId, type, "", cohortDescription, Collections.emptyList(), annotationSetList, -1, null)
                        .setName(cohortName);
                cohorts.add(catalogManager.getCohortManager().create(studyStr, cohort, null, sessionId));
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
            params = ObjectUtils.defaultIfNull(params, new CohortParameters());
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            if (StringUtils.isNotEmpty(variableSetId)) {
                variableSet = variableSetId;
            }

//            List<AnnotationSet> annotationSetList = new ArrayList<>();
//            if (params.annotationSets != null) {
//                for (CommonModels.AnnotationSetParams annotationSet : params.annotationSets) {
//                    if (annotationSet != null) {
//                        annotationSetList.add(annotationSet.toAnnotationSet(studyId, catalogManager.getStudyManager(), sessionId));
//                    }
//                }
//            }

            String cohortId = StringUtils.isEmpty(params.id) ? params.name : params.id;
            String cohortName = StringUtils.isEmpty(params.name) ? cohortId : params.name;
            return createCohort(studyStr, cohortId, cohortName, params.type, variableSet, params.description, params.samples,
                    params.annotationSets, variableName);
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
            @ApiImplicitParam(name = Constants.FLATTENED_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response infoSample(
            @ApiParam(value = "Comma separated list of cohort names or ids up to a maximum of 100", required = true)
            @PathParam("cohorts") String cohortsStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
                    + "exception whenever one of the entries looked for cannot be shown for whichever reason",
                    defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            query.remove("study");

            List<String> cohortList = getIdList(cohortsStr);
            List<DataResult<Cohort>> cohortQueryResult = cohortManager.get(studyStr, cohortList, queryOptions, silent, sessionId);
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
    public Response searchCohorts(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "DEPRECATED: Name of the cohort") @QueryParam("name") String name,
            @ApiParam(value = "Cohort type") @QueryParam("type") Study.Type type,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)")
                @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)")
                @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "Sample list") @QueryParam("samples") String samplesStr,
            @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount,
            @ApiParam(value = "Release value") @QueryParam("release") String release) {
        try {
            query.remove("study");

            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);
            DataResult<Cohort> queryResult;
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
    @ApiOperation(value = "Get samples from cohort [DEPRECATED]", position = 3, response = Sample[].class,
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
            return createOkResponse(cohortManager.getSamples(studyStr, cohortStr, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private DataResult<Cohort> createCohort(String studyStr, String cohortName, Study.Type type, String cohortDescription,
                                             List<AnnotationSet> annotationSetList, Query query, QueryOptions queryOptions)
            throws CatalogException {
        //TODO CHANGE THIS for can insert the name also id(number)
        DataResult<Sample> queryResult = catalogManager.getSampleManager().search(studyStr, query, queryOptions, sessionId);
        //TODO FOR THIS. Its possible change the param query to a String
        Cohort cohort = new Cohort(cohortName, type, "", cohortDescription, queryResult.getResults(), annotationSetList, -1, null)
                .setName(cohortName);
        return catalogManager.getCohortManager().create(studyStr, cohort, null, sessionId);
    }

    @POST
    @Path("/{cohort}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some cohort attributes", position = 4)
    public Response updateByPost(
            @ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", defaultValue = "ADD")
            @QueryParam("annotationSetsAction") ParamUtils.UpdateAction annotationSetsAction,
            @ApiParam(value = "params") CohortUpdateParams params) {
        try {
            Map<String, Object> actionMap = new HashMap<>();

            if (annotationSetsAction == null) {
                annotationSetsAction = ParamUtils.UpdateAction.ADD;
            }

            actionMap.put(CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
            queryOptions.put(Constants.ACTIONS, actionMap);

            return createOkResponse(catalogManager.getCohortManager().update(studyStr, cohortStr, params, queryOptions,
                    sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{cohort}/annotationSets/{annotationSet}/annotations/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update annotations from an annotationSet")
    public Response updateAnnotations(
            @ApiParam(value = "Cohort id", required = true) @PathParam("cohort") String cohortStr,
            @ApiParam(value = "Study [[user@]project:]study.") @QueryParam("study") String studyStr,
            @ApiParam(value = "AnnotationSet id to be updated.") @PathParam("annotationSet") String annotationSetId,
            @ApiParam(value = "Action to be performed: ADD to add new annotations; REPLACE to replace the value of an already existing "
                    + "annotation; SET to set the new list of annotations removing any possible old annotations; REMOVE to remove some "
                    + "annotations; RESET to set some annotations to the default value configured in the corresponding variables of the "
                    + "VariableSet if any.", defaultValue = "ADD") @QueryParam("action") ParamUtils.CompleteUpdateAction action,
            @ApiParam(value = "Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key "
                    + "'remove' containing the comma separated variables to be removed as a value when the action is REMOVE or a json "
                    + "with only the key 'reset' containing the comma separated variables that will be set to the default value"
                    + " when the action is RESET") Map<String, Object> updateParams) {
        try {
            if (action == null) {
                action = ParamUtils.CompleteUpdateAction.ADD;
            }
            return createOkResponse(catalogManager.getCohortManager().updateAnnotations(studyStr, cohortStr, annotationSetId,
                    updateParams, action, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/delete")
    @ApiOperation(value = "Delete existing cohorts")
    public Response delete(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Cohort id") @QueryParam("id") String id,
            @ApiParam(value = "Cohort name") @QueryParam("name") String name,
            @ApiParam(value = "Cohort type") @QueryParam("type") Study.Type type,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "Sample list") @QueryParam("samples") String samplesStr,
            @ApiParam(value = "Release value") @QueryParam("release") String release) {
        try {
            query.remove("study");
            return createOkResponse(cohortManager.delete(studyStr, query, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohort}/annotationsets/search")
    @ApiOperation(value = "Search annotation sets [DEPRECATED]", hidden = true, position = 11, notes = "Use /cohorts/search instead")
    public Response searchAnnotationSetGET(
            @ApiParam(value = "cohortId", required = true) @PathParam("cohort") String cohortStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Variable set id") @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Annotation, e.g: key1=value(,key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            Cohort cohort = cohortManager.get(studyStr, cohortStr, CohortManager.INCLUDE_COHORT_IDS, sessionId).first();

            Query query = new Query(CohortDBAdaptor.QueryParams.UID.key(), cohort.getUid());

            if (StringUtils.isEmpty(annotation)) {
                if (StringUtils.isNotEmpty(variableSet)) {
                    annotation = Constants.VARIABLE_SET + "=" + variableSet;
                }
            } else {
                if (StringUtils.isNotEmpty(variableSet)) {
                    String[] annotationsSplitted = StringUtils.split(annotation, ",");
                    List<String> annotationList = new ArrayList<>(annotationsSplitted.length);
                    for (String auxAnnotation : annotationsSplitted) {
                        String[] split = StringUtils.split(auxAnnotation, ":");
                        if (split.length == 1) {
                            annotationList.add(variableSet + ":" + auxAnnotation);
                        } else {
                            annotationList.add(auxAnnotation);
                        }
                    }
                    annotation = StringUtils.join(annotationList, ";");
                }
            }
            query.putIfNotEmpty(Constants.ANNOTATION, annotation);

            DataResult<Cohort> search = cohortManager.search(studyStr, query, new QueryOptions(Constants.FLATTENED_ANNOTATIONS, asMap),
                    sessionId);
            if (search.getNumResults() == 1) {
                return createOkResponse(new DataResult<>(search.getTime(), search.getEvents(), search.first().getAnnotationSets().size(),
                        search.first().getAnnotationSets(), search.first().getAnnotationSets().size()));
            } else {
                return createOkResponse(search);
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohorts}/annotationsets")
    @ApiOperation(value = "Return all the annotation sets of the cohort [DEPRECATED]", hidden = true, position = 12,
            notes = "Use /cohorts/search instead")
    public Response getAnnotationSet(
            @ApiParam(value = "Comma separated list of cohort Ids up to a maximum of 100", required = true) @PathParam("cohorts") String cohortsStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap,
            @ApiParam(value = "Annotation set name. If provided, only chosen annotation set will be shown") @QueryParam("name") String annotationsetName,
            @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
                    + "exception whenever one of the entries looked for cannot be shown for whichever reason",
                    defaultValue = "false") @QueryParam("silent") boolean silent) throws WebServiceException {
        try {
            List<DataResult<Cohort>> queryResults = cohortManager.get(studyStr, getIdList(cohortsStr), null, sessionId);

            Query query = new Query(CohortDBAdaptor.QueryParams.UID.key(),
                    queryResults.stream().map(DataResult::first).map(Cohort::getUid).collect(Collectors.toList()));
            QueryOptions queryOptions = new QueryOptions(Constants.FLATTENED_ANNOTATIONS, asMap);

            if (StringUtils.isNotEmpty(annotationsetName)) {
                query.append(Constants.ANNOTATION, Constants.ANNOTATION_SET_NAME + "=" + annotationsetName);
                queryOptions.put(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + "." + annotationsetName);
            }

            DataResult<Cohort> search = cohortManager.search(studyStr, query, queryOptions, sessionId);
            if (search.getNumResults() == 1) {
                return createOkResponse(new DataResult<>(search.getTime(), search.getEvents(), search.first().getAnnotationSets().size(),
                        search.first().getAnnotationSets(), search.first().getAnnotationSets().size()));
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
        public String id;

        @Deprecated
        public String name;

        @ApiModelProperty
        public Map<String, Object> annotations;
    }

    @POST
    @Path("/{cohort}/annotationsets/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create an annotation set for the cohort [DEPRECATED]", hidden = true, position = 13,
            notes = "Use /{cohort}/update instead")
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

            String annotationSetId = StringUtils.isEmpty(params.id) ? params.name : params.id;
            cohortManager.update(studyStr, cohortStr, new CohortUpdateParams().setAnnotationSets(Collections.singletonList(
                    new AnnotationSet(annotationSetId, variableSet, params.annotations))), QueryOptions.empty(), sessionId);
            DataResult<Cohort> cohortQueryResult = cohortManager.get(studyStr, cohortStr, new QueryOptions(QueryOptions.INCLUDE,
                    Constants.ANNOTATION_SET_NAME + "." + annotationSetId), sessionId);
            List<AnnotationSet> annotationSets = cohortQueryResult.first().getAnnotationSets();
            DataResult<AnnotationSet> queryResult = new DataResult<>(cohortQueryResult.getTime(), Collections.emptyList(),
                    annotationSets.size(), annotationSets, annotationSets.size());
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group cohorts by several fields", position = 24, hidden = true,
            notes = "Only group by categorical variables. Grouping by continuous variables might cause unexpected behaviour")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "count", value = "Count the number of elements matching the group", dataType = "boolean",
                    paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Maximum number of documents (groups) to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "50")
    })
    public Response groupBy(
            @ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
            @QueryParam("fields") String fields,
            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @DefaultValue("") @QueryParam("studyId")
                    String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Comma separated list of ids.") @QueryParam("id") String ids,
            @ApiParam(value = "DEPRECATED: Comma separated list of names.", required = false) @DefaultValue("") @QueryParam("name")
                    String names,
            @ApiParam(value = "Comma separated Type values.", required = false) @DefaultValue("") @QueryParam("type") String type,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") String status,
            @ApiParam(value = "creationDate", required = false) @DefaultValue("") @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Comma separated sampleIds", required = false) @DefaultValue("") @QueryParam("sampleIds") String sampleIds) {
        try {
            query.remove("study");
            query.remove("fields");

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            DataResult result = cohortManager.groupBy(studyStr, query, fields, queryOptions, sessionId);
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
            @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
                    + "exception whenever one of the entries looked for cannot be shown for whichever reason",
                    defaultValue = "false") @QueryParam("silent") boolean silent) {
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
            ObjectUtils.defaultIfNull(params, new CohortAcl());
            AclParams aclParams = new AclParams(params.getPermissions(), params.getAction());
            List<String> idList = getIdList(params.cohort, false);
            return createOkResponse(cohortManager.updateAcl(studyStr, idList, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/stats")
    @ApiOperation(value = "Fetch catalog cohort stats", position = 15, hidden = true, response = QueryResponse.class)
    public Response getStats(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr,
            @ApiParam(value = "Type") @QueryParam("type") String type,
            @ApiParam(value = "Creation year") @QueryParam("creationYear") String creationYear,
            @ApiParam(value = "Creation month (JANUARY, FEBRUARY...)") @QueryParam("creationMonth") String creationMonth,
            @ApiParam(value = "Creation day") @QueryParam("creationDay") String creationDay,
            @ApiParam(value = "Creation day of week (MONDAY, TUESDAY...)") @QueryParam("creationDayOfWeek") String creationDayOfWeek,
            @ApiParam(value = "Number of samples") @QueryParam("numSamples") String numSamples,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Release") @QueryParam("release") String release,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove("study");
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            FacetQueryResult queryResult = catalogManager.getCohortManager().facet(studyStr, query, queryOptions, defaultStats, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Fetch catalog cohort stats", position = 15, response = QueryResponse.class)
    public Response getAggregationStats(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Type") @QueryParam("type") String type,
            @ApiParam(value = "Creation year") @QueryParam("creationYear") String creationYear,
            @ApiParam(value = "Creation month (JANUARY, FEBRUARY...)") @QueryParam("creationMonth") String creationMonth,
            @ApiParam(value = "Creation day") @QueryParam("creationDay") String creationDay,
            @ApiParam(value = "Creation day of week (MONDAY, TUESDAY...)") @QueryParam("creationDayOfWeek") String creationDayOfWeek,
            @ApiParam(value = "Number of samples") @QueryParam("numSamples") String numSamples,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Release") @QueryParam("release") String release,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove("study");
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            FacetQueryResult queryResult = catalogManager.getCohortManager().facet(studyStr, query, queryOptions, defaultStats, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    protected static class CohortParameters {
        public String id;
        @Deprecated
        public String name;
        public Study.Type type;
        public String description;
        public List<String> samples;
        public List<AnnotationSet> annotationSets;
        public Map<String, Object> attributes;
    }

}
