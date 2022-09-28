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

import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.tools.annotations.*;
import org.apache.commons.lang3.ObjectUtils;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.analysis.cohort.CohortTsvAnnotationLoader;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CohortManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.cohort.*;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.TsvAnnotationParams;
import org.opencb.opencga.core.models.job.Job;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.QueryParam;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @POST
    @Path("/create")
    @ApiOperation(value = "Create a cohort", notes = "A cohort can be created by providing a list of SampleIds, " +
            "or providing a categorical variable (both variableSet and variable). " +
            "If none of this is given, an empty cohort will be created.", response = Cohort.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response createCohort(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @Deprecated
            @ApiParam(value = "Deprecated: Use /generate web service and filter by annotation") @QueryParam("variableSet") String variableSet,
            @Deprecated
            @ApiParam(value = "Deprecated: Use /generate web service and filter by annotation") @QueryParam("variable") String variableName,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing cohort information", required = true) CohortCreateParams params) {
        try {
            params = ObjectUtils.defaultIfNull(params, new CohortCreateParams());
            return createOkResponse(cohortManager.create(studyStr, params, variableSet, variableName, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/generate")
    @ApiOperation(value = "Create a cohort based on a sample query", response = Cohort.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response generateCohort(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            /* Sample search query params */
            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.SAMPLE_SOMATIC_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_SOMATIC_PARAM) Boolean somatic,
            @ApiParam(value = ParamConstants.SAMPLE_INDIVIDUAL_ID_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_INDIVIDUAL_ID_PARAM) String individual,
            @ApiParam(value = ParamConstants.SAMPLE_FILE_IDS_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_FILE_IDS_PARAM) String fileIds,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.STATUS_DESCRIPTION) @QueryParam(ParamConstants.STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.PHENOTYPES_DESCRIPTION) @QueryParam(ParamConstants.PHENOTYPES_PARAM) String phenotypes,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam(Constants.ANNOTATION) String annotation,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.SNAPSHOT_PARAM) int snapshot,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            /* End Sample search query params */
            @ApiParam(value = "JSON containing cohort information", required = true) CohortGenerateParams params) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(cohortManager.generate(studyStr, query, params.toCohort(), queryOptions, token));
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
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.COHORT_IDS_DESCRIPTION) @QueryParam(ParamConstants.COHORT_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.COHORT_NAMES_DESCRIPTION) @QueryParam(ParamConstants.COHORT_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.COHORT_UUIDS_DESCRIPTION) @QueryParam(ParamConstants.COHORT_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.COHORT_TYPE_DESCRIPTION) @QueryParam(ParamConstants.COHORT_TYPE_PARAM) Enums.CohortType type,
            @ApiParam(value = ParamConstants.COHORT_CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.COHORT_CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.COHORT_MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.COHORT_MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.COHORT_DELETED_DESCRIPTION) @QueryParam(ParamConstants.COHORT_DELETED_PARAM) boolean deleted,
            @ApiParam(value = ParamConstants.COHORT_STATUS_DESCRIPTION) @QueryParam(ParamConstants.COHORT_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.COHORT_INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.COHORT_INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.COHORT_ANNOTATION_DESCRIPTION) @QueryParam(ParamConstants.COHORT_ANNOTATION_PARAM) String annotation,
            @ApiParam(value = ParamConstants.COHORT_ACL_DESCRIPTION) @QueryParam(ParamConstants.COHORT_ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.COHORT_SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.COHORT_SAMPLES_PARAM) String samplesStr,
            @ApiParam(value = ParamConstants.COHORT_NUMBER_OF_SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.COHORT_NUMBER_OF_SAMPLES_PARAM) String numSamples,
            @ApiParam(value = ParamConstants.COHORT_RELEASE_DESCRIPTION) @QueryParam(ParamConstants.COHORT_RELEASE_PARAM) String release) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(catalogManager.getCohortManager().search(studyStr, query, queryOptions, token));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/distinct")
    @ApiOperation(value = "Cohort distinct method", response = Object.class)
    public Response distinct(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.COHORT_IDS_DESCRIPTION) @QueryParam(ParamConstants.COHORT_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.COHORT_NAMES_DESCRIPTION) @QueryParam(ParamConstants.COHORT_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.COHORT_UUIDS_DESCRIPTION) @QueryParam(ParamConstants.COHORT_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.COHORT_TYPE_DESCRIPTION) @QueryParam(ParamConstants.COHORT_TYPE_PARAM) Enums.CohortType type,
            @ApiParam(value = ParamConstants.COHORT_CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.COHORT_CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.COHORT_MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.COHORT_MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.COHORT_DELETED_DESCRIPTION) @QueryParam(ParamConstants.COHORT_DELETED_PARAM) boolean deleted,
            @ApiParam(value = ParamConstants.COHORT_STATUS_DESCRIPTION) @QueryParam(ParamConstants.COHORT_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.COHORT_INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.COHORT_INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.COHORT_ANNOTATION_DESCRIPTION) @QueryParam(ParamConstants.COHORT_ANNOTATION_PARAM) String annotation,
            @ApiParam(value = ParamConstants.COHORT_ACL_DESCRIPTION) @QueryParam(ParamConstants.COHORT_ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.COHORT_SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.COHORT_SAMPLES_PARAM) String samplesStr,
            @ApiParam(value = ParamConstants.COHORT_NUMBER_OF_SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.COHORT_NUMBER_OF_SAMPLES_PARAM) String numSamples,
            @ApiParam(value = ParamConstants.COHORT_RELEASE_DESCRIPTION) @QueryParam(ParamConstants.COHORT_RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);
            return createOkResponse(cohortManager.distinct(studyStr, field, query, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @POST
//    @Path("/update")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "Update some cohort attributes", hidden = true, response = Cohort.class)
//    public Response updateQuery(
//            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
//                @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
//            @ApiParam(value = ParamConstants.COHORT_ID_DESCRIPTION) @QueryParam("id") String id,
//            @ApiParam(value = ParamConstants.COHORT_NAME_DESCRIPTION) @QueryParam("name") String name,
//            @ApiParam(value = ParamConstants.COHORT_TYPE_DESCRIPTION) @QueryParam("type") Enums.CohortType type,
//            @ApiParam(value = ParamConstants.COHORT_STATUS_DESCRIPTION) @QueryParam("status") String status,
//            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
//            @ApiParam(value = "Sample list") @QueryParam("samples") String samplesStr,
//            @ApiParam(value = "Release value") @QueryParam("release") String release,
//            @ApiParam(value = "Action to be performed if the array of samples is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
//                @QueryParam("samplesAction") ParamUtils.BasicUpdateAction samplesAction,
//            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
//                @QueryParam("annotationSetsAction") ParamUtils.BasicUpdateAction annotationSetsAction,
//            @ApiParam(value = "body") CohortUpdateParams params) {
//        try {
//            query.remove(ParamConstants.STUDY_PARAM);
//
//            Map<String, Object> actionMap = new HashMap<>();
//            if (annotationSetsAction == null) {
//                annotationSetsAction = ParamUtils.BasicUpdateAction.ADD;
//            }
//            if (samplesAction == null) {
//                samplesAction = ParamUtils.BasicUpdateAction.ADD;
//            }
//
//            actionMap.put(CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
//            actionMap.put(CohortDBAdaptor.QueryParams.SAMPLES.key(), samplesAction);
//            queryOptions.put(Constants.ACTIONS, actionMap);
//
//            return createOkResponse(catalogManager.getCohortManager().update(studyStr, query, params, queryOptions, token));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @POST
    @Path("/{cohorts}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some cohort attributes", response = Cohort.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response updateByPost(
            @ApiParam(value = "Comma separated list of cohort ids", required = true) @PathParam("cohorts") String cohorts,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Action to be performed if the array of samples is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
            @QueryParam("samplesAction") ParamUtils.BasicUpdateAction samplesAction,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
            @QueryParam("annotationSetsAction") ParamUtils.BasicUpdateAction annotationSetsAction,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "body") CohortUpdateParams params) {
        try {
            Map<String, Object> actionMap = new HashMap<>();

            if (annotationSetsAction == null) {
                annotationSetsAction = ParamUtils.BasicUpdateAction.ADD;
            }
            if (samplesAction == null) {
                samplesAction = ParamUtils.BasicUpdateAction.ADD;
            }

            actionMap.put(CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
            actionMap.put(CohortDBAdaptor.QueryParams.SAMPLES.key(), samplesAction);
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
    @ApiOperation(value = "Return the acl of the cohort. If member is provided, it will only return the acl for the member.",
            response = AclEntryList.class)
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
    @ApiOperation(value = "Update the set of permissions granted for the member", response = AclEntryList.class)
    public Response updateAcl(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = ParamConstants.ACL_ACTION_DESCRIPTION, required = true, defaultValue = "ADD") @QueryParam(ParamConstants.ACL_ACTION_PARAM) ParamUtils.AclAction action,
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
