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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;

/**
 * Created by pfurio on 03/05/17.
 */
@Path("/{version}/families")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Families (BETA)", position = 8, description = "Methods for working with 'families' endpoint")
public class FamilyWSServer extends OpenCGAWSServer {

    private FamilyManager familyManager;

    public FamilyWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        familyManager = catalogManager.getFamilyManager();
    }

    @GET
    @Path("/{families}/info")
    @ApiOperation(value = "Get family information", position = 1, response = Family.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response infoFamily(@ApiParam(value = "Comma separated list of family IDs or names", required = true)
                               @PathParam("families") String familyStr,
                               @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                               @QueryParam("study") String studyStr) {
        try {
            QueryResult<Family> familyQueryResult = familyManager.get(studyStr, familyStr, queryOptions, sessionId);
            // We parse the query result to create one queryresult per family
            List<QueryResult<Family>> queryResultList = new ArrayList<>(familyQueryResult.getNumResults());
            for (Family family : familyQueryResult.getResult()) {
                queryResultList.add(new QueryResult<>(family.getName() + "-" + family.getId(), familyQueryResult.getDbTime(), 1, -1,
                        familyQueryResult.getWarningMsg(), familyQueryResult.getErrorMsg(), Arrays.asList(family)));
            }

            return createOkResponse(queryResultList);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Multi-study search that allows the user to look for families from from different studies of the same project "
            + "applying filters.", position = 4, response = Sample[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response search(@ApiParam(value = "Study [[user@]project:]{study1,study2|*}  where studies and project can be either the id or"
                                   + " alias.") @QueryParam("study") String studyStr,
                           @ApiParam(value = "Family name") @QueryParam("name") String name,
                           @ApiParam(value = "Parental consanguinity") @QueryParam("parentalConsanguinity") Boolean parentalConsanguinity,
                           @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("mother") String mother,
                           @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("father") String father,
                           @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("member") String member,
                           @ApiParam(value = "Comma separated list of disease ids") @QueryParam("diseases") String diseases,
                           @ApiParam(value = "annotationsetName") @QueryParam("annotationsetName") String annotationsetName,
                           @ApiParam(value = "variableSetId", hidden = true) @QueryParam("variableSetId") String variableSetId,
                           @ApiParam(value = "variableSet") @QueryParam("variableSet") String variableSet,
                           @ApiParam(value = "Annotation, e.g: key1=value(,key2=value)") @QueryParam("annotation") String annotation,
                           @ApiParam(value = "Release value") @QueryParam("release") String release,
                           @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount) {
        try {
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
    @ApiOperation(value = "Create family", position = 2, response = Family.class)
    public Response createFamilyPOST(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value="JSON containing family information", required = true) CreateFamilyPOST family) {
        try {
            QueryResult<Family> queryResult = familyManager.create(studyStr,
                    family.toFamily(studyStr, catalogManager.getStudyManager(), sessionId), queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{family}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some family attributes using POST method", position = 6)
    public Response updateByPost(@ApiParam(value = "familyId", required = true) @PathParam("family") String familyStr,
                                 @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                 @QueryParam("study") String studyStr,
                                 @ApiParam(value = "params", required = true) FamilyPOST parameters) {
        try {
            ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(parameters));

            if (params.size() == 0) {
                throw new CatalogException("Missing parameters to update.");
            }

            QueryResult<Family> queryResult = catalogManager.getFamilyManager().update(studyStr, familyStr, params, queryOptions,
                    sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{family}/annotationsets/search")
    @ApiOperation(value = "Search annotation sets", position = 11)
    public Response searchAnnotationSetGET(
            @ApiParam(value = "familyId", required = true) @PathParam("family") String familyStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Variable set id or name", hidden = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = "Variable set id or name", required = true) @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Annotation, e.g: key1=value(,key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            if (StringUtils.isNotEmpty(variableSetId)) {
                variableSet = variableSetId;
            }
            if (asMap) {
                return createOkResponse(familyManager.searchAnnotationSetAsMap(familyStr, studyStr, variableSet, annotation, sessionId));
            } else {
                return createOkResponse(familyManager.searchAnnotationSet(familyStr, studyStr, variableSet, annotation, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{family}/annotationsets")
    @ApiOperation(value = "Return the annotation sets of the family", position = 12)
    public Response getAnnotationSet(
            @ApiParam(value = "familyId", required = true) @PathParam("family") String familyStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap,
            @ApiParam(value = "Annotation set name. If provided, only chosen annotation set will be shown") @QueryParam("name") String annotationsetName) {
        try {
            if (asMap) {
                if (StringUtils.isNotEmpty(annotationsetName)) {
                    return createOkResponse(familyManager.getAnnotationSetAsMap(familyStr, studyStr, annotationsetName, sessionId));
                } else {
                    return createOkResponse(familyManager.getAllAnnotationSetsAsMap(familyStr, studyStr, sessionId));
                }
            } else {
                if (StringUtils.isNotEmpty(annotationsetName)) {
                    return createOkResponse(familyManager.getAnnotationSet(familyStr, studyStr, annotationsetName, sessionId));
                } else {
                    return createOkResponse(familyManager.getAllAnnotationSets(familyStr, studyStr, sessionId));
                }
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{family}/annotationsets/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create an annotation set for the family", position = 13)
    public Response annotateFamilyPOST(
            @ApiParam(value = "FamilyId", required = true) @PathParam("family") String familyStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Variable set id or name", hidden = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = "Variable set id or name", required = true) @QueryParam("variableSet") String variableSet,
            @ApiParam(value="JSON containing the annotation set name and the array of annotations. The name should be unique for the "
                    + "family", required = true) CohortWSServer.AnnotationsetParameters params) {
        try {
            if (StringUtils.isNotEmpty(variableSetId)) {
                variableSet = variableSetId;
            }
            QueryResult<AnnotationSet> queryResult = familyManager.createAnnotationSet(familyStr, studyStr, variableSet, params.name,
                    params.annotations, Collections.emptyMap(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{family}/annotationsets/{annotationsetName}/delete")
    @ApiOperation(value = "Delete the annotation set or the annotations within the annotation set", position = 14)
    public Response deleteAnnotationGET(@ApiParam(value = "familyId", required = true) @PathParam("family") String familyStr,
                                        @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                        @QueryParam("study") String studyStr,
                                        @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName") String annotationsetName,
                                        @ApiParam(value = "[NOT IMPLEMENTED] Comma separated list of annotation names to be deleted", required = false) @QueryParam("annotations") String annotations) {
        try {
            QueryResult<AnnotationSet> queryResult;
            if (annotations != null) {
                queryResult = familyManager.deleteAnnotations(familyStr, studyStr, annotationsetName, annotations, sessionId);
            } else {
                queryResult = familyManager.deleteAnnotationSet(familyStr, studyStr, annotationsetName, sessionId);
            }
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{family}/annotationsets/{annotationsetName}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update the annotations", position = 15)
    public Response updateAnnotationGET(
            @ApiParam(value = "familyId", required = true) @PathParam("family") String familyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName") String annotationsetName,
            @ApiParam(value="JSON containing key:value annotations to update", required = true) Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult = familyManager.updateAnnotationSet(familyIdStr, studyStr, annotationsetName,
                    annotations, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{families}/acl")
    @ApiOperation(value = "Returns the acl of the families. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of family IDs or names", required = true) @PathParam("families")
                                    String familyIdsStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member) {
        try {
            if (StringUtils.isEmpty(member)) {
                return createOkResponse(familyManager.getAcls(studyStr, familyIdsStr, sessionId));
            } else {
                return createOkResponse(familyManager.getAcl(studyStr, familyIdsStr, member, sessionId));
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class FamilyAcl extends AclParams {
        public String family;
    }

    @POST
    @Path("/acl/{memberIds}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("memberIds") String memberId,
            @ApiParam(value="JSON containing the parameters to add ACLs", required = true) FamilyWSServer.FamilyAcl params) {
        try {
            AclParams familyAclParams = new AclParams(params.getPermissions(), params.getAction());
            return createOkResponse(familyManager.updateAcl(studyStr, params.family, memberId, familyAclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private static class RelativesPOST {
        public IndividualWSServer.IndividualPOST member;
        public IndividualWSServer.IndividualPOST father;
        public IndividualWSServer.IndividualPOST mother;
        public List<String> diseases;
        public List<String> carrier;
        public boolean parentalConsanguinity;

        private Relatives toRelatives(String studyStr, StudyManager studyManager, String sessionId) throws CatalogException {
            Individual realIndividual = member != null ? member.toIndividual(studyStr, studyManager, sessionId) : null;
            Individual realFather = father != null ? father.toIndividual(studyStr, studyManager, sessionId) : null;
            Individual realMother = mother != null ? mother.toIndividual(studyStr, studyManager, sessionId) : null;

            return new Relatives(realIndividual, realFather, realMother, diseases, carrier, parentalConsanguinity);
        }

    }

    private static class FamilyPOST {
        public String name;
        public String description;

        public List<OntologyTerm> diseases;
        public List<RelativesPOST> members;

        public Map<String, Object> attributes;
    }

    private static class CreateFamilyPOST extends FamilyPOST {

        public List<CommonModels.AnnotationSetParams> annotationSets;

        public Family toFamily(String studyStr, StudyManager studyManager, String sessionId) throws CatalogException {
            List<AnnotationSet> annotationSetList = new ArrayList<>();
            if (annotationSets != null) {
                for (CommonModels.AnnotationSetParams annotationSet : annotationSets) {
                    if (annotationSet != null) {
                        annotationSetList.add(annotationSet.toAnnotationSet(studyStr, studyManager, sessionId));
                    }
                }
            }

            List<Relatives> relatives = new ArrayList<>(members.size());
            for (RelativesPOST member : members) {
                relatives.add(member.toRelatives(studyStr, studyManager, sessionId));
            }

            return new Family(name, diseases, relatives, description, annotationSetList, attributes);
        }
    }

}
