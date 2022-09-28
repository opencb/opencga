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

import org.apache.commons.lang3.ObjectUtils;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.family.FamilyTsvAnnotationLoader;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.common.TsvAnnotationParams;
import org.opencb.opencga.core.models.family.*;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.tools.annotations.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 03/05/17.
 */
@Path("/{apiVersion}/families")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Families", description = "Methods for working with 'families' endpoint")
public class FamilyWSServer extends OpenCGAWSServer {

    private FamilyManager familyManager;

    public FamilyWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        familyManager = catalogManager.getFamilyManager();
    }

    @GET
    @Path("/{families}/info")
    @ApiOperation(value = "Get family information", response = Family.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.FLATTEN_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response infoFamily(
            @ApiParam(value = ParamConstants.FAMILIES_DESCRIPTION, required = true) @PathParam("families") String familyStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.FAMILY_VERSION_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_VERSION_PARAM) String version,
            @ApiParam(value = "Boolean to retrieve deleted families", defaultValue = "false") @QueryParam("deleted") boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("families");

            List<String> familyList = getIdList(familyStr);
            DataResult<Family> familyQueryResult = familyManager.get(studyStr, familyList, query, queryOptions, true, token);
            return createOkResponse(familyQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search families", response = Family.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.FLATTEN_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.FAMILY_ID_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.FAMILY_NAME_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.FAMILY_UUID_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.FAMILY_MEMBERS_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_MEMBERS_PARAM) String members,
            @ApiParam(value = ParamConstants.FAMILY_EXPECTED_SIZE_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_EXPECTED_SIZE_PARAM) Integer expectedSize,
            @ApiParam(value = ParamConstants.FAMILY_SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_SAMPLES_PARAM) String samples,
            @ApiParam(value = ParamConstants.FAMILY_PHENOTYPES_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_PHENOTYPES_PARAM) String phenotypes,
            @ApiParam(value = ParamConstants.FAMILY_DISORDERS_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_DISORDERS_PARAM) String disorders,
            @ApiParam(value = ParamConstants.FAMILY_CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.FAMILY_MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.FAMILY_DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.FAMILY_DELETED_PARAM) boolean deleted,
            @ApiParam(value = ParamConstants.FAMILY_INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.FAMILY_STATUS_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.FAMILY_ANNOTATION_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_ANNOTATION_PARAM) String annotation,
            @ApiParam(value = ParamConstants.FAMILY_ACL_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.FAMILY_RELEASE_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.FAMILY_SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_SNAPSHOT_PARAM) Integer snapshot) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(familyManager.search(studyStr, query, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/distinct")
    @ApiOperation(value = "Family distinct method", response = Object.class)
    public Response distinct(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.FAMILY_ID_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.FAMILY_NAME_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.FAMILY_UUID_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.FAMILY_MEMBERS_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_MEMBERS_PARAM) String members,
            @ApiParam(value = ParamConstants.FAMILY_EXPECTED_SIZE_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_EXPECTED_SIZE_PARAM) Integer expectedSize,
            @ApiParam(value = ParamConstants.FAMILY_SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_SAMPLES_PARAM) String samples,
            @ApiParam(value = ParamConstants.FAMILY_PHENOTYPES_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_PHENOTYPES_PARAM) String phenotypes,
            @ApiParam(value = ParamConstants.FAMILY_DISORDERS_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_DISORDERS_PARAM) String disorders,
            @ApiParam(value = ParamConstants.FAMILY_CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.FAMILY_MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.FAMILY_DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.FAMILY_DELETED_PARAM) boolean deleted,
            @ApiParam(value = ParamConstants.FAMILY_INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.FAMILY_STATUS_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.FAMILY_ANNOTATION_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_ANNOTATION_PARAM) String annotation,
            @ApiParam(value = ParamConstants.FAMILY_ACL_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.FAMILY_RELEASE_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.FAMILY_SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.FAMILY_SNAPSHOT_PARAM) Integer snapshot,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);
            return createOkResponse(familyManager.distinct(studyStr, field, query, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create family and the individual objects if they do not exist", response = Family.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response createFamilyPOST(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM)
                    String studyStr,
            @ApiParam(value = "Comma separated list of member ids to be associated to the created family") @QueryParam("members")
                    String members,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing family information", required = true) FamilyCreateParams family) {
        try {
            family = ObjectUtils.defaultIfNull(family, new FamilyCreateParams());
            DataResult<Family> queryResult = familyManager.create(studyStr,
                    family.toFamily(), getIdListOrEmpty(members), queryOptions, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @POST
//    @Path("/update")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "Update some family attributes", hidden = true, response = Family.class)
//    public Response updateByQuery(
//            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
//            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
//            @ApiParam(value = "Family id") @QueryParam("id") String id,
//            @ApiParam(value = "Family name") @QueryParam("name") String name,
//            @ApiParam(value = "Parental consanguinity") @QueryParam("parentalConsanguinity") Boolean parentalConsanguinity,
//            @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("members") String members,
//            @ApiParam(value = "Comma separated list of phenotype ids or names") @QueryParam("phenotypes") String phenotypes,
//            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
//            @QueryParam("release") String release,
//            @ApiParam(value = "Create a new version of family", defaultValue = "false") @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
//            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
//            @QueryParam("annotationSetsAction") ParamUtils.BasicUpdateAction annotationSetsAction,
//            @ApiParam(value = "body") FamilyUpdateParams parameters) {
//        try {
//            query.remove(ParamConstants.STUDY_PARAM);
//            if (annotationSetsAction == null) {
//                annotationSetsAction = ParamUtils.BasicUpdateAction.ADD;
//            }
//
//            Map<String, Object> actionMap = new HashMap<>();
//            actionMap.put(FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
//            queryOptions.put(Constants.ACTIONS, actionMap);
//
//            DataResult<Family> queryResult = catalogManager.getFamilyManager().update(studyStr, query, parameters, true, queryOptions, token);
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }


    @POST
    @Path("/{families}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some family attributes", response = Family.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response updateByPost(
            @ApiParam(value = "Comma separated list of family ids", required = true) @PathParam("families") String familyStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.FAMILY_UPDATE_ROLES_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.FAMILY_UPDATE_ROLES_PARAM) boolean updateRoles,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD") @QueryParam("annotationSetsAction") ParamUtils.BasicUpdateAction annotationSetsAction,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "body") FamilyUpdateParams parameters) {
        try {
            if (annotationSetsAction == null) {
                annotationSetsAction = ParamUtils.BasicUpdateAction.ADD;
            }

            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
            queryOptions.put(Constants.ACTIONS, actionMap);

            List<String> familyIds = getIdList(familyStr);

            DataResult<Family> queryResult = catalogManager.getFamilyManager().update(studyStr, familyIds, parameters, true, queryOptions, token);
            return createOkResponse(queryResult);
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

            return createOkResponse(catalogManager.getFamilyManager().loadTsvAnnotations(studyStr, variableSetId, path, params,
                    additionalParams, FamilyTsvAnnotationLoader.ID, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{family}/annotationSets/{annotationSet}/annotations/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update annotations from an annotationSet", response = Family.class)
    public Response updateAnnotations(
            @ApiParam(value = "Family id", required = true) @PathParam("family") String familyStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_ID) @PathParam("annotationSet") String annotationSetId,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_UPDATE_ACTION_DESCRIPTION, allowableValues = "ADD,SET,REMOVE,RESET,REPLACE", defaultValue = "ADD")
            @QueryParam("action") ParamUtils.CompleteUpdateAction action,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_UPDATE_PARAMS_DESCRIPTION) Map<String, Object> updateParams) {
        try {
            if (action == null) {
                action = ParamUtils.CompleteUpdateAction.ADD;
            }

            return createOkResponse(catalogManager.getFamilyManager().updateAnnotations(studyStr, familyStr, annotationSetId,
                    updateParams, action, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/{families}/delete")
    @ApiOperation(value = "Delete existing families", response = Family.class)
    public Response delete(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of family ids") @PathParam("families") String families) {
        try {
            List<String> familyIds = getIdList(families);
            return createOkResponse(familyManager.delete(studyStr, familyIds, queryOptions, true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{families}/acl")
    @ApiOperation(value = "Returns the acl of the families. If member is provided, it will only return the acl for the member.", response = AclEntryList.class)
    public Response getAcls(@ApiParam(value = ParamConstants.FAMILIES_DESCRIPTION, required = true) @PathParam("families")
                                    String familyIdsStr,
                            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION,
                                    defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        try {
            List<String> idList = getIdList(familyIdsStr);
            return createOkResponse(familyManager.getAcls(studyStr, idList, member, silent, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", response = AclEntryList.class)
    public Response updateAcl(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberList,
            @ApiParam(value = ParamConstants.ACL_ACTION_DESCRIPTION, required = true, defaultValue = "ADD") @QueryParam(ParamConstants.ACL_ACTION_PARAM) ParamUtils.AclAction action,
            @ApiParam(value = "Propagate family permissions to related individuals and samples", defaultValue = "NO") @QueryParam("propagate") FamilyAclParams.Propagate propagate,
            @ApiParam(value = "JSON containing the parameters to add ACLs", required = true) FamilyAclUpdateParams params) {
        try {
            FamilyAclParams aclParams = new FamilyAclParams(params.getPermissions(), params.getFamily(), params.getIndividual(), params.getSample(), propagate);
            return createOkResponse(familyManager.updateAcl(studyStr, aclParams, memberList, action, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Fetch catalog family stats", response = FacetField.class)
    public Response getAggregationStats(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Creation year") @QueryParam("creationYear") String creationYear,
            @ApiParam(value = "Creation month (JANUARY, FEBRUARY...)") @QueryParam("creationMonth") String creationMonth,
            @ApiParam(value = "Creation day") @QueryParam("creationDay") String creationDay,
            @ApiParam(value = "Creation day of week (MONDAY, TUESDAY...)") @QueryParam("creationDayOfWeek") String creationDayOfWeek,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Phenotypes") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "Release") @QueryParam("release") String release,
            @ApiParam(value = "Version") @QueryParam("version") String version,
            @ApiParam(value = "Number of members") @QueryParam("numMembers") String numMembers,
            @ApiParam(value = "Expected size") @QueryParam("expectedSize") String expectedSize,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            DataResult<FacetField> queryResult = catalogManager.getFamilyManager().facet(studyStr, query, queryOptions, defaultStats,
                    token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
