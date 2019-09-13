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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.swagger.annotations.*;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.models.pedigree.Multiples;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AnnotationSetManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.catalog.models.update.FamilyUpdateParams;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.Family;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Location;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.server.WebServiceException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.QueryParam;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

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
    @ApiOperation(value = "Get family information", position = 1, response = Family.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = Constants.FLATTENED_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response infoFamily(
            @ApiParam(value = "Comma separated list of family IDs or names up to a maximum of 100", required = true) @PathParam("families") String familyStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Family version") @QueryParam("version") Integer version,
            @ApiParam(value = "Fetch all family versions", defaultValue = "false") @QueryParam(Constants.ALL_VERSIONS) boolean allVersions,
            @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
                    + "exception whenever one of the entries looked for cannot be shown for whichever reason", defaultValue = "false")
                @QueryParam("silent") boolean silent) {
        try {
            query.remove("study");
            query.remove("families");

            List<String> familyList = getIdList(familyStr);
            List<QueryResult<Family>> familyQueryResult = familyManager.get(studyStr, familyList, query, queryOptions, silent, sessionId);
            return createOkResponse(familyQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search families", position = 4, response = Family[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", defaultValue = "false", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = Constants.FLATTENED_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias.")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Family name") @QueryParam("name") String name,
            @ApiParam(value = "Parental consanguinity") @QueryParam("parentalConsanguinity") Boolean parentalConsanguinity,
            @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("members") String members,
            @ApiParam(value = "Comma separated list of sample ids or names") @QueryParam("samples") String samples,
            @ApiParam(value = "Comma separated list of phenotype ids or names") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "Comma separated list of disorder ids or names") @QueryParam("disorders") String disorders,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)")
                @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)")
                @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "DEPRECATED: Use annotation queryParam this way: annotationSet[=|==|!|!=]{annotationSetName}")
            @QueryParam("annotationsetName") String annotationsetName,
            @ApiParam(value = "DEPRECATED: Use annotation queryParam this way: variableSet[=|==|!|!=]{variableSetId}")
            @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount,
            @ApiParam(value = "Release value (Current release from the moment the families were first created)")
            @QueryParam("release") String release,
            @ApiParam(value = "Snapshot value (Latest version of families in the specified release)") @QueryParam("snapshot")
                    int snapshot) {
        try {
            query.remove("study");

            List<String> annotationList = new ArrayList<>();
            if (StringUtils.isNotEmpty(annotation)) {
                annotationList.add(annotation);
            }
            if (StringUtils.isNotEmpty(variableSet)) {
                annotationList.add(Constants.VARIABLE_SET + "=" + variableSet);
            }
            if (StringUtils.isNotEmpty(annotationsetName)) {
                annotationList.add(Constants.ANNOTATION_SET_NAME + "=" + annotationsetName);
            }
            if (!annotationList.isEmpty()) {
                query.put(Constants.ANNOTATION, StringUtils.join(annotationList, ";"));
            }

            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);
            QueryResult<Family> queryResult;
            if (count) {
                queryResult = familyManager.count(studyStr, query, sessionId);
            } else {
                queryResult = familyManager.search(studyStr, query, queryOptions, sessionId);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create family and the individual objects if they do not exist", position = 2, response = Family.class)
    public Response createFamilyPOST(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Comma separated list of member ids to be associated to the created family") @QueryParam("members")
                    String members,
            @ApiParam(value = "JSON containing family information", required = true) FamilyPOST family) {
        try {
            family = ObjectUtils.defaultIfNull(family, new FamilyPOST());
            QueryResult<Family> queryResult = familyManager.create(studyStr,
                    family.toFamily(studyStr, catalogManager.getStudyManager(), sessionId), getIdList(members), queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{family}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some family attributes", position = 6,
            notes = "The entire family is returned after the modification. Using include/exclude query parameters is encouraged to "
                    + "avoid slowdowns when sending unnecessary information where possible")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query")
    })
    public Response updateByPost(
            @ApiParam(value = "familyId", required = true) @PathParam("family") String familyStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Create a new version of family", defaultValue = "false")
            @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
            @ApiParam(value = "Update all the individual references from the family to point to their latest versions",
                    defaultValue = "false") @QueryParam("updateIndividualVersion") boolean refresh,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", defaultValue = "ADD")
            @QueryParam("annotationSetsAction") ParamUtils.UpdateAction annotationSetsAction,
            @ApiParam(value = "params") FamilyUpdateParams parameters) {
        try {
            if (annotationSetsAction == null) {
                annotationSetsAction = ParamUtils.UpdateAction.ADD;
            }

            queryOptions.put(Constants.REFRESH, refresh);
            queryOptions.remove("updateIndividualVersion");
            query.remove("updateIndividualVersion");

            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
            queryOptions.put(Constants.ACTIONS, actionMap);

            QueryResult<Family> queryResult = catalogManager.getFamilyManager().update(studyStr, familyStr, parameters, queryOptions,
                    sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{family}/annotationSets/{annotationSet}/annotations/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update annotations from an annotationSet")
    public Response updateAnnotations(
            @ApiParam(value = "Family id", required = true) @PathParam("family") String familyStr,
            @ApiParam(value = "Study [[user@]project:]study.") @QueryParam("study") String studyStr,
            @ApiParam(value = "AnnotationSet id to be updated.") @PathParam("annotationSet") String annotationSetId,
            @ApiParam(value = "Action to be performed: ADD to add new annotations; REPLACE to replace the value of an already existing "
                    + "annotation; SET to set the new list of annotations removing any possible old annotations; REMOVE to remove some "
                    + "annotations; RESET to set some annotations to the default value configured in the corresponding variables of the "
                    + "VariableSet if any.", defaultValue = "ADD") @QueryParam("action") ParamUtils.CompleteUpdateAction action,
            @ApiParam(value = "Create a new version of family", defaultValue = "false") @QueryParam(Constants.INCREMENT_VERSION)
                    boolean incVersion,
            @ApiParam(value = "Update all the individual references from the family to point to their latest versions",
                    defaultValue = "false") @QueryParam("updateSampleVersion") boolean refresh,
            @ApiParam(value = "Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key "
                    + "'remove' containing the comma separated variables to be removed as a value when the action is REMOVE or a json "
                    + "with only the key 'reset' containing the comma separated variables that will be set to the default value"
                    + " when the action is RESET") Map<String, Object> updateParams) {
        try {
            if (action == null) {
                action = ParamUtils.CompleteUpdateAction.ADD;
            }
            queryOptions.put(Constants.REFRESH, refresh);

            return createOkResponse(catalogManager.getFamilyManager().updateAnnotations(studyStr, familyStr, annotationSetId,
                    updateParams, action, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/delete")
    @ApiOperation(value = "Delete existing families")
    public Response delete(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Family id") @QueryParam("id") String id,
            @ApiParam(value = "Family name") @QueryParam("name") String name,
            @ApiParam(value = "Parental consanguinity") @QueryParam("parentalConsanguinity") Boolean parentalConsanguinity,
            @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("members") String members,
            @ApiParam(value = "Comma separated list of phenotype ids or names") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,
            @QueryParam("release") String release) {
        try {
            query.remove("study");
            return createOkResponse(familyManager.delete(studyStr, query, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group families by several fields", position = 10, hidden = true,
            notes = "Only group by categorical variables. Grouping by continuous variables might cause unexpected behaviour")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "count", value = "Count the number of elements matching the group", dataType = "boolean",
                    paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Maximum number of documents (groups) to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "50")
    })
    public Response groupBy(
            @ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("") @QueryParam("fields") String fields,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Family name") @QueryParam("name") String name,
            @ApiParam(value = "Parental consanguinity") @QueryParam("parentalConsanguinity") Boolean parentalConsanguinity,
            @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("members") String members,
            @ApiParam(value = "Comma separated list of sample ids or names") @QueryParam("samples") String samples,
            @ApiParam(value = "Comma separated list of phenotype ids or names") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "DEPRECATED: Use annotation queryParam this way: annotationSet[=|==|!|!=]{annotationSetName}")
            @QueryParam("annotationsetName") String annotationsetName,
            @ApiParam(value = "DEPRECATED: Use annotation queryParam this way: variableSet[=|==|!|!=]{variableSetId}")
            @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "Release value (Current release from the moment the families were first created)") @QueryParam("release") String release,
            @ApiParam(value = "Snapshot value (Latest version of families in the specified release)") @QueryParam("snapshot") int snapshot) {
        try {
            query.remove("study");
            query.remove("fields");

            QueryResult result = familyManager.groupBy(studyStr, query, fields, queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{family}/annotationsets/search")
    @ApiOperation(value = "Search annotation sets [DEPRECATED]", hidden = true, position = 11, notes = "Use /families/search instead")
    public Response searchAnnotationSetGET(
            @ApiParam(value = "familyId", required = true) @PathParam("family") String familyStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Variable set id") @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Annotation, e.g: key1=value(,key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            Family family = familyManager.get(studyStr, familyStr, FamilyManager.INCLUDE_FAMILY_IDS, sessionId).first();
            Query query = new Query(FamilyDBAdaptor.QueryParams.UID.key(), family.getUid());

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

            QueryResult<Family> search = familyManager.search(studyStr, query, new QueryOptions(Constants.FLATTENED_ANNOTATIONS, asMap),
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
    @Path("/{families}/annotationsets")
    @ApiOperation(value = "Return the annotation sets of the family [DEPRECATED]", hidden = true, position = 12,
            notes = "Use /families/search instead")
    public Response getAnnotationSet(
            @ApiParam(value = "Comma separated list of family IDs or names up to a maximum of 100", required = true) @PathParam("families") String familiesStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap,
            @ApiParam(value = "Annotation set name. If provided, only chosen annotation set will be shown") @QueryParam("name") String annotationsetName,
            @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
                    + "exception whenever one of the entries looked for cannot be shown for whichever reason", defaultValue = "false")
                @QueryParam("silent") boolean silent) throws WebServiceException {
        try {
            List<QueryResult<Family>> queryResults = familyManager.get(studyStr, getIdList(familiesStr), null, sessionId);

            Query query = new Query(FamilyDBAdaptor.QueryParams.UID.key(),
                    queryResults.stream().map(QueryResult::first).map(Family::getUid).collect(Collectors.toList()));
            QueryOptions queryOptions = new QueryOptions(Constants.FLATTENED_ANNOTATIONS, asMap);

            if (StringUtils.isNotEmpty(annotationsetName)) {
                query.append(Constants.ANNOTATION, Constants.ANNOTATION_SET_NAME + "=" + annotationsetName);
                queryOptions.put(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + "." + annotationsetName);
            }

            QueryResult<Family> search = familyManager.search(studyStr, query, queryOptions, sessionId);
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

    @POST
    @Path("/{family}/annotationsets/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create an annotation set for the family [DEPRECATED]", hidden = true, position = 13,
            notes = "Use /{family}/update instead")
    public Response annotateFamilyPOST(
            @ApiParam(value = "FamilyId", required = true) @PathParam("family") String familyStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Variable set id or name", hidden = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = "Variable set id or name", required = true) @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "JSON containing the annotation set name and the array of annotations. The name should be unique for the "
                    + "family", required = true) CohortWSServer.AnnotationsetParameters params) {
        try {
            if (StringUtils.isNotEmpty(variableSetId)) {
                variableSet = variableSetId;
            }
            String annotationSetId = StringUtils.isEmpty(params.id) ? params.name : params.id;

            familyManager.update(studyStr, familyStr, new FamilyUpdateParams().setAnnotationSets(Collections.singletonList(
                    new AnnotationSet(annotationSetId, variableSet, params.annotations))), QueryOptions.empty(), sessionId);
            QueryResult<Family> familyQueryResult = familyManager.get(studyStr, familyStr, new QueryOptions(QueryOptions.INCLUDE,
                    Constants.ANNOTATION_SET_NAME + "." + annotationSetId), sessionId);
            List<AnnotationSet> annotationSets = familyQueryResult.first().getAnnotationSets();
            QueryResult<AnnotationSet> queryResult = new QueryResult<>(familyStr, familyQueryResult.getDbTime(), annotationSets.size(),
                    annotationSets.size(), "", "", annotationSets);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{families}/acl")
    @ApiOperation(value = "Returns the acl of the families. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of family IDs or names up to a maximum of 100", required = true) @PathParam("families")
                                    String familyIdsStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
                                    + "exception whenever one of the entries looked for cannot be shown for whichever reason",
                                    defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            List<String> idList = getIdList(familyIdsStr);
            return createOkResponse(familyManager.getAcls(studyStr, idList, member, silent, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class FamilyAcl extends AclParams {
        public String family;
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = "JSON containing the parameters to add ACLs", required = true) FamilyWSServer.FamilyAcl params) {
        try {
            params = ObjectUtils.defaultIfNull(params, new FamilyAcl());
            AclParams familyAclParams = new AclParams(params.getPermissions(), params.getAction());
            List<String> idList = getIdList(params.family, false);
            return createOkResponse(familyManager.updateAcl(studyStr, idList, memberId, familyAclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/stats")
    @ApiOperation(value = "Fetch catalog family stats", position = 15, hidden = true, response = QueryResponse.class)
    public Response getStats(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr,
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
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove("study");
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            FacetQueryResult queryResult = catalogManager.getFamilyManager().facet(studyStr, query, queryOptions, defaultStats, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Fetch catalog family stats", position = 15, response = QueryResponse.class)
    public Response getAggregationStats(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
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
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove("study");
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            FacetQueryResult queryResult = catalogManager.getFamilyManager().facet(studyStr, query, queryOptions, defaultStats, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    protected static class IndividualPOST {
        public String id;
        public String name;

        public String father;
        public String mother;
        public Multiples multiples;
        public Location location;

        public IndividualProperty.Sex sex;
        public String ethnicity;
        public Boolean parentalConsanguinity;
        public Individual.Population population;
        public String dateOfBirth;
        public IndividualProperty.KaryotypicSex karyotypicSex;
        public IndividualProperty.LifeStatus lifeStatus;
        public IndividualProperty.AffectationStatus affectationStatus;
        public List<AnnotationSet> annotationSets;
        public List<Phenotype> phenotypes;
        public List<Disorder> disorders;
        public Map<String, Object> attributes;


        public Individual toIndividual(String studyStr, StudyManager studyManager, String sessionId) throws CatalogException {
//            List<AnnotationSet> annotationSetList = new ArrayList<>();
//            if (annotationSets != null) {
//                for (CommonModels.AnnotationSetParams annotationSet : annotationSets) {
//                    if (annotationSet != null) {
//                        annotationSetList.add(annotationSet.toAnnotationSet(studyId, studyManager, sessionId));
//                    }
//                }
//            }

            String individualId = StringUtils.isEmpty(id) ? name : id;
            String individualName = StringUtils.isEmpty(name) ? individualId : name;
            return new Individual(individualId, individualName, father != null ? new Individual().setId(father) : null,
                    mother != null ? new Individual().setId(mother) : null, multiples, location,
                    sex, karyotypicSex, ethnicity, population, lifeStatus, affectationStatus, dateOfBirth,
                    null, parentalConsanguinity != null ? parentalConsanguinity : false, 1, annotationSets, phenotypes, disorders)
                    .setAttributes(attributes);
        }

        public Individual toIndividualUpdate() {
            String individualId = StringUtils.isEmpty(id) ? name : id;
            String individualName = StringUtils.isEmpty(name) ? individualId : name;

            Individual individual = new Individual()
                    .setId(individualId)
                    .setName(individualName)
                    .setFather(father != null ? new Individual().setId(father) : null)
                    .setMother(mother != null ? new Individual().setId(mother) : null)
                    .setMultiples(multiples)
                    .setSex(sex)
                    .setLocation(location)
                    .setKaryotypicSex(karyotypicSex)
                    .setEthnicity(ethnicity)
                    .setPopulation(population)
                    .setLifeStatus(lifeStatus)
                    .setAffectationStatus(affectationStatus)
                    .setDateOfBirth(dateOfBirth)
                    .setParentalConsanguinity(parentalConsanguinity != null ? parentalConsanguinity : false)
                    .setPhenotypes(phenotypes)
                    .setDisorders(disorders)
                    .setAttributes(attributes);
            individual.setAnnotationSets(annotationSets);
            return individual;
        }
    }

    private static class FamilyPOST {
        public String id;
        public String name;
        public String description;

        public List<Phenotype> phenotypes;
        public List<Disorder> disorders;
        public List<IndividualPOST> members;

        public Integer expectedSize;

        public Map<String, Object> attributes;
        public List<AnnotationSet> annotationSets;

        public Family toFamily(String studyStr, StudyManager studyManager, String sessionId) throws CatalogException {

            List<Individual> relatives = null;
            if (members != null) {
                relatives = new ArrayList<>(members.size());
                for (IndividualPOST member : members) {
                    relatives.add(member.toIndividual(studyStr, studyManager, sessionId));
                }
            }

            String familyId = StringUtils.isEmpty(id) ? name : id;
            String familyName = StringUtils.isEmpty(name) ? familyId : name;
            int familyExpectedSize = expectedSize != null ? expectedSize : -1;
            return new Family(familyId, familyName, phenotypes, disorders, relatives, description, familyExpectedSize, annotationSets,
                    attributes);
        }
    }
}
