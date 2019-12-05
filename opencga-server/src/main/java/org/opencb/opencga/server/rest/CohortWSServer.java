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
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CohortManager;
import org.opencb.opencga.catalog.models.update.CohortUpdateParams;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.server.WebServiceException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.QueryParam;
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
                DataResult<Sample> queryResult = catalogManager.getSampleManager().get(studyStr, sampleIdList, null, token);
                List<Sample> sampleList = queryResult.getResults();
                Cohort cohort = new Cohort(cohortId, type, "", cohortDescription, sampleList, annotationSetList, -1, null)
                        .setName(cohortName);
                DataResult<Cohort> cohortQueryResult = catalogManager.getCohortManager().create(studyStr, cohort, null, token);
                cohorts.add(cohortQueryResult);
            } else if (StringUtils.isNotEmpty(variableSetId)) {
                VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(studyStr, variableSetId, null, token).first();
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
                    QueryOptions samplesQOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.samples.id");
                    Query samplesQuery = new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key() + "." + variableName, s)
                            .append("variableSetId", variableSet.getUid());

                    cohorts.add(createCohort(studyStr, cohortName + "_" + s, type, cohortDescription, annotationSetList, samplesQuery,
                            samplesQOptions));
                }
            } else {
                //Create empty cohort
                Cohort cohort = new Cohort(cohortId, type, "", cohortDescription, Collections.emptyList(), annotationSetList, -1, null)
                        .setName(cohortName);
                cohorts.add(catalogManager.getCohortManager().create(studyStr, cohort, null, token));
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
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM)
                    String studyStr,
            @ApiParam(value = ParamConstants.VARIANBLE_SET_DESCRIPTION, hidden = true) @QueryParam("variableSetId")
                    String variableSetId,
            @ApiParam(value = ParamConstants.VARIANBLE_SET_DESCRIPTION) @QueryParam("variableSet") String variableSet,
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
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = Constants.FLATTENED_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response infoSample(
            @ApiParam(value = ParamConstants.COHORTS_DESCRIPTION, required = true)
                @PathParam("cohorts") String cohortsStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Boolean to retrieve deleted cohorts", defaultValue = "false") @QueryParam("deleted") boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);

            List<String> cohortList = getIdList(cohortsStr);
            DataResult<Cohort> cohortQueryResult = cohortManager.get(studyStr, cohortList, new Query("deleted", deleted), queryOptions, true, token);
            return createOkResponse(cohortQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search cohorts", position = 2, response = Cohort.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = Constants.FLATTENED_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response searchCohorts(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "DEPRECATED: Name of the cohort") @QueryParam("name") String name,
            @ApiParam(value = ParamConstants.COHORT_TYPE_DESCRIPTION) @QueryParam("type") Study.Type type,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION)
                @QueryParam("creationDate") String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION)
                @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "Boolean to retrieve deleted cohorts", defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
            @ApiParam(value = "Sample list") @QueryParam("samples") String samplesStr,
            @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount,
            @ApiParam(value = "Release value") @QueryParam("release") String release) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);

            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);
            DataResult<Cohort> queryResult;
            if (count) {
                queryResult = catalogManager.getCohortManager().count(studyStr, query, token);
            } else {
                queryResult = catalogManager.getCohortManager().search(studyStr, query, queryOptions, token);
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
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response getSamples(@ApiParam(value = "Cohort id or name", required = true) @PathParam("cohort") String cohortStr,
                               @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                               @QueryParam(ParamConstants.STUDY_PARAM) String studyStr) {
        try {
            return createOkResponse(cohortManager.getSamples(studyStr, cohortStr, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private DataResult<Cohort> createCohort(String studyStr, String cohortName, Study.Type type, String cohortDescription,
                                             List<AnnotationSet> annotationSetList, Query query, QueryOptions queryOptions)
            throws CatalogException {
        //TODO CHANGE THIS for can insert the name also id(number)
        DataResult<Sample> queryResult = catalogManager.getSampleManager().search(studyStr, query, queryOptions, token);
        //TODO FOR THIS. Its possible change the param query to a String
        Cohort cohort = new Cohort(cohortName, type, "", cohortDescription, queryResult.getResults(), annotationSetList, -1, null)
                .setName(cohortName);
        return catalogManager.getCohortManager().create(studyStr, cohort, null, token);
    }

    @POST
    @Path("/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some cohort attributes")
    public Response updateQuery(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.COHORT_ID_DESCRIPTION) @QueryParam("id") String id,
            @ApiParam(value = ParamConstants.COHORT_NAME_DESCRIPTION) @QueryParam("name") String name,
            @ApiParam(value = ParamConstants.COHORT_TYPE_DESCRIPTION) @QueryParam("type") Study.Type type,
            @ApiParam(value = ParamConstants.COHORT_STATUS_DESCRIPTION) @QueryParam("status") String status,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
            @ApiParam(value = "Sample list") @QueryParam("samples") String samplesStr,
            @ApiParam(value = "Release value") @QueryParam("release") String release,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", defaultValue = "ADD")
                @QueryParam("annotationSetsAction") ParamUtils.UpdateAction annotationSetsAction,
            @ApiParam(value = "params") CohortUpdateParams params) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);

            Map<String, Object> actionMap = new HashMap<>();
            if (annotationSetsAction == null) {
                annotationSetsAction = ParamUtils.UpdateAction.ADD;
            }

            actionMap.put(CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
            queryOptions.put(Constants.ACTIONS, actionMap);

            return createOkResponse(catalogManager.getCohortManager().update(studyStr, query, params, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{cohorts}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some cohort attributes", position = 4)
    public Response updateByPost(
            @ApiParam(value = "Comma separated list of cohort ids", required = true) @PathParam("cohorts") String cohorts,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
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

            List<String> cohortIds = getIdList(cohorts);

            return createOkResponse(catalogManager.getCohortManager().update(studyStr, cohortIds, params, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{cohort}/annotationSets/{annotationSet}/annotations/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update annotations from an annotationSet")
    public Response updateAnnotations(
            @ApiParam(value = ParamConstants.COHORT_ID_DESCRIPTION, required = true) @PathParam("cohort") String cohortStr,
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_ID) @PathParam("annotationSet") String annotationSetId,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_UPDATE_ACTION_DESCRIPTION, defaultValue = "ADD") @QueryParam("action") ParamUtils.CompleteUpdateAction action,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_UPDATE_PARAMS_DESCRIPTION) Map<String, Object> updateParams) {
        try {
            if (action == null) {
                action = ParamUtils.CompleteUpdateAction.ADD;
            }
            return createOkResponse(catalogManager.getCohortManager().updateAnnotations(studyStr, cohortStr, annotationSetId,
                    updateParams, action, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/delete")
    @ApiOperation(value = "Delete cohorts")
    public Response deleteQuery(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.COHORT_ID_DESCRIPTION) @QueryParam("id") String id,
            @ApiParam(value = ParamConstants.COHORT_NAME_DESCRIPTION) @QueryParam("name") String name,
            @ApiParam(value = ParamConstants.COHORT_TYPE_DESCRIPTION) @QueryParam("type") Study.Type type,
            @ApiParam(value = ParamConstants.COHORT_STATUS_DESCRIPTION) @QueryParam("status") String status,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
            @ApiParam(value = "Sample list") @QueryParam("samples") String samplesStr,
            @ApiParam(value = "Release value") @QueryParam("release") String release) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(cohortManager.delete(studyStr, query, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("{cohorts}/delete")
    @ApiOperation(value = "Delete cohorts")
    public Response deleteList(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of cohort ids") @PathParam("cohorts") String cohorts) {
        try {
            List<String> cohortIds = getIdList(cohorts);
            return createOkResponse(cohortManager.delete(studyStr, cohortIds, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohort}/annotationsets/search")
    @ApiOperation(value = "Search annotation sets [DEPRECATED]", hidden = true, position = 11, notes = "Use /cohorts/search instead")
    public Response searchAnnotationSetGET(
            @ApiParam(value = ParamConstants.COHORT_DESCRIPTION, required = true) @PathParam("cohort") String cohortStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.VARIANBLE_SET_DESCRIPTION) @QueryParam("variableSet") String variableSet,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
            @ApiParam(value = ParamConstants.ANNOTATION_AS_MAP_DESCRIPTION, defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            Cohort cohort = cohortManager.get(studyStr, cohortStr, CohortManager.INCLUDE_COHORT_IDS, token).first();

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
                    token);
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
            @ApiParam(value = ParamConstants.COHORTS_DESCRIPTION, required = true) @PathParam("cohorts") String cohortsStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.ANNOTATION_AS_MAP_DESCRIPTION, defaultValue = "false") @QueryParam("asMap") boolean asMap,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_NAME) @QueryParam("name") String annotationsetName,
            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION,
                    defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) throws WebServiceException {
        try {
            DataResult<Cohort> queryResult = cohortManager.get(studyStr, getIdList(cohortsStr), null, token);

            Query query = new Query(CohortDBAdaptor.QueryParams.UID.key(),
                    queryResult.getResults().stream().map(Cohort::getUid).collect(Collectors.toList()));
            QueryOptions queryOptions = new QueryOptions(Constants.FLATTENED_ANNOTATIONS, asMap);

            if (StringUtils.isNotEmpty(annotationsetName)) {
                query.append(Constants.ANNOTATION, Constants.ANNOTATION_SET_NAME + "=" + annotationsetName);
                queryOptions.put(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + "." + annotationsetName);
            }

            DataResult<Cohort> search = cohortManager.search(studyStr, query, queryOptions, token);
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
            @ApiParam(value = ParamConstants.COHORT_DESCRIPTION, required = true) @PathParam("cohort") String cohortStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM)
                    String studyStr,
            @ApiParam(value = ParamConstants.VARIANBLE_SET_DESCRIPTION, hidden = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = ParamConstants.VARIANBLE_SET_DESCRIPTION, required = true) @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "JSON containing the annotation set name and the array of annotations. The name should be unique for the "
                    + "cohort", required = true) AnnotationsetParameters params) {
        try {
            if (StringUtils.isNotEmpty(variableSetId)) {
                variableSet = variableSetId;
            }

            String annotationSetId = StringUtils.isEmpty(params.id) ? params.name : params.id;
            cohortManager.update(studyStr, cohortStr,
                    new CohortUpdateParams().setAnnotationSets(Collections.singletonList(
                            new AnnotationSet(annotationSetId, variableSet, params.annotations))), QueryOptions.empty(), token);
            DataResult<Cohort> cohortQueryResult = cohortManager.get(studyStr, cohortStr, new QueryOptions(QueryOptions.INCLUDE,
                    Constants.ANNOTATION_SET_NAME + "." + annotationSetId), token);
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
            @ApiImplicitParam(name = QueryOptions.COUNT, value = "Count the number of elements matching the group", dataType = "boolean",
                    paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Maximum number of documents (groups) to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "50")
    })
    public Response groupBy(
            @ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
            @QueryParam("fields") String fields,
            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @DefaultValue("") @QueryParam("studyId")
                    String studyIdStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of ids.") @QueryParam("id") String ids,
            @ApiParam(value = "DEPRECATED: Comma separated list of names.", required = false) @DefaultValue("") @QueryParam("name")
                    String names,
            @ApiParam(value = "Comma separated Type values.", required = false) @DefaultValue("") @QueryParam("type") String type,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
            @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") String status,
            @ApiParam(value = "creationDate", required = false) @DefaultValue("") @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Comma separated sampleIds", required = false) @DefaultValue("") @QueryParam("sampleIds") String sampleIds) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("fields");

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            DataResult result = cohortManager.groupBy(studyStr, query, fields, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohorts}/acl")
    @ApiOperation(value = "Return the acl of the cohort. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(
            @ApiParam(value = ParamConstants.COHORTS_DESCRIPTION, required = true) @PathParam("cohorts") String cohortIdsStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "User or group id") @QueryParam("member") String member,
            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION,
                    defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        try {
            List<String> idList = getIdList(cohortIdsStr);
            return createOkResponse(cohortManager.getAcls(studyStr, idList, member, silent, token));
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
            @ApiParam(value = ParamConstants.COHORTS_DESCRIPTION, required = true) @PathParam("cohort") String cohortIdStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value = "JSON containing one of the keys 'add', 'set' or 'remove'", required = true) StudyWSServer.MemberAclUpdateOld params) {
        try {
            AclParams aclParams = getAclParams(params.add, params.remove, params.set);
            List<String> idList = getIdList(cohortIdStr);
            return createOkResponse(cohortManager.updateAcl(studyStr, idList, memberId, aclParams, token));
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
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = "JSON containing the parameters to add ACLs", required = true) CohortAcl params) {
        try {
            ObjectUtils.defaultIfNull(params, new CohortAcl());
            AclParams aclParams = new AclParams(params.getPermissions(), params.getAction());
            List<String> idList = getIdList(params.cohort, false);
            return createOkResponse(cohortManager.updateAcl(studyStr, idList, memberId, aclParams, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/stats")
    @ApiOperation(value = "Fetch catalog cohort stats", position = 15, hidden = true, response = QueryResponse.class)
    public Response getStats(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Type") @QueryParam("type") String type,
            @ApiParam(value = "Creation year") @QueryParam("creationYear") String creationYear,
            @ApiParam(value = "Creation month (JANUARY, FEBRUARY...)") @QueryParam("creationMonth") String creationMonth,
            @ApiParam(value = "Creation day") @QueryParam("creationDay") String creationDay,
            @ApiParam(value = "Creation day of week (MONDAY, TUESDAY...)") @QueryParam("creationDayOfWeek") String creationDayOfWeek,
            @ApiParam(value = "Number of samples") @QueryParam("numSamples") String numSamples,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Release") @QueryParam("release") String release,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            DataResult<FacetField> queryResult = catalogManager.getCohortManager().facet(studyStr, query, queryOptions, defaultStats,
                    token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Fetch catalog cohort stats", position = 15, response = QueryResponse.class)
    public Response getAggregationStats(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Type") @QueryParam("type") String type,
            @ApiParam(value = "Creation year") @QueryParam("creationYear") String creationYear,
            @ApiParam(value = "Creation month (JANUARY, FEBRUARY...)") @QueryParam("creationMonth") String creationMonth,
            @ApiParam(value = "Creation day") @QueryParam("creationDay") String creationDay,
            @ApiParam(value = "Creation day of week (MONDAY, TUESDAY...)") @QueryParam("creationDayOfWeek") String creationDayOfWeek,
            @ApiParam(value = "Number of samples") @QueryParam("numSamples") String numSamples,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Release") @QueryParam("release") String release,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            DataResult<FacetField> queryResult = catalogManager.getCohortManager().facet(studyStr, query, queryOptions, defaultStats,
                    token);
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
