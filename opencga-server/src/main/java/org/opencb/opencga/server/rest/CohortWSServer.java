/*
 * Copyright 2015-2020 OpenCB
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
import org.opencb.opencga.analysis.cohort.CohortTsvAnnotationLoader;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CohortManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortAclUpdateParams;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatus;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.TsvAnnotationParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.QueryParam;
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

    public CohortWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        cohortManager = catalogManager.getCohortManager();
    }

    private Response createCohort(String studyStr, String variableSetId, CohortCreateParams cohortParams, String variableName) {
        try {
            List<DataResult<Cohort>> cohorts = new LinkedList<>();
            if (StringUtils.isNotEmpty(variableName) && ListUtils.isNotEmpty(cohortParams.getSamples())) {
                return createErrorResponse("", "Can only create a cohort given list of sampleIds or a categorical variable name");
            }

            if (ListUtils.isNotEmpty(cohortParams.getSamples())) {
                DataResult<Sample> queryResult = catalogManager.getSampleManager().get(studyStr, cohortParams.getSamples(), null, token);
                List<Sample> sampleList = queryResult.getResults();
                Cohort cohort = new Cohort(cohortParams.getId(), cohortParams.getType(), "", cohortParams.getDescription(), sampleList,
                        cohortParams.getAnnotationSets(), 1,
                        cohortParams.getStatus() != null ? cohortParams.getStatus().toCustomStatus() : new CustomStatus(),
                        null, cohortParams.getAttributes());
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

                    cohorts.add(createCohort(studyStr, cohortParams.getId() + "_" + s, cohortParams.getType(), cohortParams.getDescription(),
                            cohortParams.getAnnotationSets(), samplesQuery, samplesQOptions));
                }
            } else {
                //Create empty cohort
                Cohort cohort = new Cohort(cohortParams.getId(), cohortParams.getType(), "", cohortParams.getDescription(),
                        Collections.emptyList(), cohortParams.getAnnotationSets(), -1, null);
                cohorts.add(catalogManager.getCohortManager().create(studyStr, cohort, null, token));
            }
            return createOkResponse(cohorts);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create a cohort", notes = "A cohort can be created by providing a list of SampleIds, " +
            "or providing a categorical variable (both variableSet and variable). " +
            "If none of this is given, an empty cohort will be created.", response = Cohort.class)
    public Response createCohort(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM)
                    String studyStr,
            @ApiParam(value = ParamConstants.VARIABLE_SET_DESCRIPTION, hidden = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = ParamConstants.VARIABLE_SET_DESCRIPTION) @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Variable name") @QueryParam("variable") String variableName,
            @ApiParam(value = "JSON containing cohort information", required = true) CohortCreateParams params) {
        try {
            params = ObjectUtils.defaultIfNull(params, new CohortCreateParams());
            if (StringUtils.isNotEmpty(variableSetId)) {
                variableSet = variableSetId;
            }

            return createCohort(studyStr, variableSet, params, variableName);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohorts}/info")
    @ApiOperation(value = "Get cohort information", response = Cohort.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.FLATTEN_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
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
    @ApiOperation(value = "Search cohorts", response = Cohort.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.FLATTEN_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response searchCohorts(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "DEPRECATED: Name of the cohort") @QueryParam("name") String name,
            @ApiParam(value = ParamConstants.COHORT_TYPE_DESCRIPTION) @QueryParam("type") Enums.CohortType type,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION)
                @QueryParam("creationDate") String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION)
                @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "Boolean to retrieve deleted cohorts", defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = "Sample list") @QueryParam("samples") String samplesStr,
            @ApiParam(value = "Release value") @QueryParam("release") String release) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(catalogManager.getCohortManager().search(studyStr, query, queryOptions, token));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    private DataResult<Cohort> createCohort(String studyStr, String cohortId, Enums.CohortType type, String cohortDescription,
                                            List<AnnotationSet> annotationSetList, Query query, QueryOptions queryOptions)
            throws CatalogException {
        DataResult<Sample> queryResult = catalogManager.getSampleManager().search(studyStr, query, queryOptions, token);
        Cohort cohort = new Cohort(cohortId, type, "", cohortDescription, queryResult.getResults(), annotationSetList, -1, null);
        return catalogManager.getCohortManager().create(studyStr, cohort, null, token);
    }

    @POST
    @Path("/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some cohort attributes", hidden = true, response = Cohort.class)
    public Response updateQuery(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.COHORT_ID_DESCRIPTION) @QueryParam("id") String id,
            @ApiParam(value = ParamConstants.COHORT_NAME_DESCRIPTION) @QueryParam("name") String name,
            @ApiParam(value = ParamConstants.COHORT_TYPE_DESCRIPTION) @QueryParam("type") Enums.CohortType type,
            @ApiParam(value = ParamConstants.COHORT_STATUS_DESCRIPTION) @QueryParam("status") String status,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
            @ApiParam(value = "Sample list") @QueryParam("samples") String samplesStr,
            @ApiParam(value = "Release value") @QueryParam("release") String release,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
                @QueryParam("annotationSetsAction") ParamUtils.UpdateAction annotationSetsAction,
            @ApiParam(value = "body") CohortUpdateParams params) {
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
    @ApiOperation(value = "Update some cohort attributes", response = Cohort.class)
    public Response updateByPost(
            @ApiParam(value = "Comma separated list of cohort ids", required = true) @PathParam("cohorts") String cohorts,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
                @QueryParam("annotationSetsAction") ParamUtils.UpdateAction annotationSetsAction,
            @ApiParam(value = "body") CohortUpdateParams params) {
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
    @Path("/annotationSets/load")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Load annotation sets from a TSV file", response = Job.class)
    public Response loadTsvAnnotations(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.VARIABLE_SET_DESCRIPTION, required = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = "Path where the TSV file is located in OpenCGA or where it should be located.", required = true)
                @QueryParam("path") String path,
            @ApiParam(value = "Flag indicating whether to create parent directories if they don't exist (only when TSV file was not previously associated).")
            @DefaultValue("false") @QueryParam("parents") boolean parents,
            @ApiParam(value = "Annotation set id. If not provided, variableSetId will be used.") @QueryParam("annotationSetId") String annotationSetId,
            @ApiParam(value = ParamConstants.TSV_ANNOTATION_DESCRIPTION) TsvAnnotationParams params) {
        try {
            ObjectMap additionalParams = new ObjectMap()
                    .append("parents", parents)
                    .append("annotationSetId", annotationSetId);

            return createOkResponse(catalogManager.getCohortManager().loadTsvAnnotations(studyStr, variableSetId, path, params,
                    additionalParams, CohortTsvAnnotationLoader.ID, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{cohort}/annotationSets/{annotationSet}/annotations/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update annotations from an annotationSet", response = Cohort.class)
    public Response updateAnnotations(
            @ApiParam(value = ParamConstants.COHORT_ID_DESCRIPTION, required = true) @PathParam("cohort") String cohortStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_ID) @PathParam("annotationSet") String annotationSetId,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_UPDATE_ACTION_DESCRIPTION, allowableValues = "ADD,SET,REMOVE,RESET,REPLACE", defaultValue = "ADD")
                @QueryParam("action") ParamUtils.CompleteUpdateAction action,
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
    @Path("/{cohorts}/delete")
    @ApiOperation(value = "Delete cohorts", response = Cohort.class)
    public Response deleteList(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of cohort ids") @PathParam("cohorts") String cohorts) {
        try {
            List<String> cohortIds = getIdList(cohorts);
            return createOkResponse(cohortManager.delete(studyStr, cohortIds, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{cohorts}/acl")
    @ApiOperation(value = "Return the acl of the cohort. If member is provided, it will only return the acl for the member.", response = Map.class)
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
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", response = Map.class)
    public Response updateAcl(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = ParamConstants.ACL_ACTION_DESCRIPTION, required = true) @QueryParam(ParamConstants.ACL_ACTION_PARAM) ParamUtils.AclAction action,
            @ApiParam(value = "JSON containing the parameters to add ACLs", required = true) CohortAclUpdateParams params) {
        try {
            ObjectUtils.defaultIfNull(params, new CohortAclUpdateParams());
            AclParams aclParams = new AclParams(params.getPermissions());
            List<String> idList = getIdList(params.getCohort(), false);
            return createOkResponse(cohortManager.updateAcl(studyStr, idList, memberId, aclParams, action, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Fetch catalog cohort stats", response = FacetField.class)
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

}
