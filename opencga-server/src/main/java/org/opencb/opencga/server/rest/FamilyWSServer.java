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
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.managers.api.IStudyManager;
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
            AbstractManager.MyResourceIds resourceIds = familyManager.getIds(familyStr, studyStr, sessionId);

            List<QueryResult<Family>> queryResults = new LinkedList<>();
            if (resourceIds.getResourceIds() != null && resourceIds.getResourceIds().size() > 0) {
                for (Long familyId : resourceIds.getResourceIds()) {
                    queryResults.add(familyManager.get(familyId, queryOptions, sessionId));
                }
            }
            return createOkResponse(queryResults);
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
                           @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("mother") String
                                       mother,
                           @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("father") String
                                       father,
                           @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("children") String
                                       children,
                           @ApiParam(value = "Comma separated list of ontology ids or names") @QueryParam("ontologies") String ontologies,
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
                                 @ApiParam(value = "params", required = true) UpdateFamilyPOST parameters) {
        try {
            AbstractManager.MyResourceId resourceId = catalogManager.getFamilyManager().getId(familyStr, studyStr, sessionId);

            ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(parameters));

            if (params.size() == 0) {
                throw new CatalogException("Missing parameters to update.");
            }

            QueryResult<Family> queryResult = catalogManager.getFamilyManager().update(resourceId.getResourceId(), params, queryOptions,
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
                return createOkResponse(catalogManager.getAllFamilyAcls(familyIdsStr, studyStr, sessionId));
            } else {
                return createOkResponse(catalogManager.getFamilyAcl(familyIdsStr, studyStr, member, sessionId));
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
            AclParams familyAclParams = new AclParams(
                    params.getPermissions(), params.getAction());
            return createOkResponse(familyManager.updateAcl(params.family, studyStr, memberId, familyAclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private static class FamilyPOST {
        public String name;

        public Boolean parentalConsanguinity;

        public String description;
        public List<OntologyTerm> ontologyTerms;
        public Map<String, Object> attributes;
    }

    private static class UpdateFamilyPOST extends FamilyPOST {
        public Long motherId;
        public Long fatherId;
        public List<Long> childrenIds;
    }

    private static class CreateFamilyPOST extends FamilyPOST {
        public IndividualWSServer.IndividualPOST mother;
        public IndividualWSServer.IndividualPOST father;
        public List<IndividualWSServer.IndividualPOST> children;
        public List<CommonModels.AnnotationSetParams> annotationSets;

        public Family toFamily(String studyStr, IStudyManager studyManager, String sessionId) throws CatalogException {
            List<Individual> childrenList = new ArrayList<>();
            if (children != null) {
                for (IndividualWSServer.IndividualPOST child : children) {
                    childrenList.add(child.toIndividual(studyStr, studyManager, sessionId));
                }
            }

            List<AnnotationSet> annotationSetList = new ArrayList<>();
            if (annotationSets != null) {
                for (CommonModels.AnnotationSetParams annotationSet : annotationSets) {
                    if (annotationSet != null) {
                        annotationSetList.add(annotationSet.toAnnotationSet(studyStr, studyManager, sessionId));
                    }
                }
            }

            return new Family(name, father != null ? father.toIndividual(studyStr, studyManager, sessionId) : null,
                    mother != null ? mother.toIndividual(studyStr, studyManager, sessionId) : null, childrenList,
                    parentalConsanguinity != null ? parentalConsanguinity : false, description, ontologyTerms, annotationSetList, 1,
                    attributes);
        }
    }

}
