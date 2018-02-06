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
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.AnnotationSetManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.server.WebServiceException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 03/05/17.
 */
@Path("/{apiVersion}/families")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Families", position = 8, description = "Methods for working with 'families' endpoint")
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
    public Response infoFamily(
            @ApiParam(value = "Comma separated list of family IDs or names up to a maximum of 100", required = true) @PathParam("families") String familyStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Family version") @QueryParam("version") Integer version,
            @ApiParam(value = "Fetch all family versions", defaultValue = "false") @QueryParam(Constants.ALL_VERSIONS) boolean allVersions,
            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
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
            @ApiParam(value = "Study [[user@]project:]{study1,study2|*}  where studies and project can be either the id or alias.")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Family name") @QueryParam("name") String name,
            @ApiParam(value = "Parental consanguinity") @QueryParam("parentalConsanguinity") Boolean parentalConsanguinity,
            @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("mother") String mother,
            @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("father") String father,
            @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("member") String member,
            @ApiParam(value = "Comma separated list of phenotype ids or names") @QueryParam("phenotypes") String phenotypes,
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
            @ApiParam(value = "JSON containing family information", required = true) CreateFamilyPOST family) {
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
    @ApiOperation(value = "Update some family attributes", position = 6)
    public Response updateByPost(
            @ApiParam(value = "familyId", required = true) @PathParam("family") String familyStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Create a new version of family", defaultValue = "false")
            @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
            @ApiParam(value = "Update all the individual references from the family to point to their latest versions",
                    defaultValue = "false") @QueryParam("updateIndividualVersion") boolean refresh,
            @ApiParam(value = "Delete a specific annotation set") @QueryParam(Constants.DELETE_ANNOTATION_SET) String deleteAnnotationSet,
            @ApiParam(value = "Delete a specific annotation. Format: annotationSetName:variable")
                @QueryParam(Constants.DELETE_ANNOTATION_SET) String deleteAnnotation,
            @ApiParam(value = "params", required = true) FamilyPOST parameters) {
        try {
            queryOptions.put(Constants.REFRESH, refresh);
            queryOptions.remove("updateIndividualVersion");
            query.remove("updateIndividualVersion");

            ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(parameters));
            ObjectMap privateMap = new ObjectMap();
            privateMap.putIfNotEmpty(AnnotationSetManager.Action.DELETE_ANNOTATION_SET.name(), deleteAnnotationSet);
            privateMap.putIfNotEmpty(AnnotationSetManager.Action.DELETE_ANNOTATION.name(), deleteAnnotation);
            if (!privateMap.isEmpty()) {
                params.put(FamilyDBAdaptor.QueryParams.PRIVATE_FIELDS.key(), privateMap);
            }

            QueryResult<Family> queryResult = catalogManager.getFamilyManager().update(studyStr, familyStr, params, queryOptions,
                    sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group families by several fields", position = 10,
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
            @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("mother") String mother,
            @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("father") String father,
            @ApiParam(value = "Comma separated list of individual ids or names") @QueryParam("member") String member,
            @ApiParam(value = "Comma separated list of phenotype ids or names") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "annotationsetName") @QueryParam("annotationsetName") String annotationsetName,
            @ApiParam(value = "variableSetId", hidden = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = "variableSet") @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Annotation, e.g: key1=value(,key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "Release value (Current release from the moment the families were first created)") @QueryParam("release") String release,
            @ApiParam(value = "Snapshot value (Latest version of families in the specified release)") @QueryParam("snapshot") int snapshot) {
        try {
            QueryResult result = familyManager.groupBy(studyStr, query, fields, queryOptions, sessionId);
            return createOkResponse(result);
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
            @ApiParam(value = "Variable set id or name", required = true) @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Annotation, e.g: key1=value(,key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            AbstractManager.MyResourceId resourceId = familyManager.getId(familyStr, studyStr, sessionId);

            Query query = new Query()
                    .append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), resourceId.getStudyId())
                    .append(FamilyDBAdaptor.QueryParams.ID.key(), resourceId.getResourceId())
                    .append(Constants.FLATTENED_ANNOTATIONS, asMap);

            String variableSetId = String.valueOf(catalogManager.getStudyManager()
                    .getVariableSetId(variableSet, String.valueOf(resourceId.getStudyId()), sessionId).getResourceId());

            if (StringUtils.isEmpty(annotation)) {
                annotation = Constants.VARIABLE_SET + "=" + variableSetId;
            } else {
                annotation += ";" + Constants.VARIABLE_SET + "=" + variableSetId;
            }
            query.append(Constants.ANNOTATION, annotation);

            QueryResult<Family> search = familyManager.search(String.valueOf(resourceId.getStudyId()), query, new QueryOptions(),
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
    @ApiOperation(value = "Return the annotation sets of the family", position = 12)
    public Response getAnnotationSet(
            @ApiParam(value = "Comma separated list of family IDs or names up to a maximum of 100", required = true) @PathParam("families") String familiesStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap,
            @ApiParam(value = "Annotation set name. If provided, only chosen annotation set will be shown") @QueryParam("name") String annotationsetName,
            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false") @QueryParam("silent") boolean silent) throws WebServiceException {
        try {
            AbstractManager.MyResourceIds resourceIds = familyManager.getIds(familiesStr, studyStr, sessionId);

            Query query = new Query()
                    .append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), resourceIds.getStudyId())
                    .append(FamilyDBAdaptor.QueryParams.ID.key(), resourceIds.getResourceIds())
                    .append(Constants.FLATTENED_ANNOTATIONS, asMap);

            if (StringUtils.isNotEmpty(annotationsetName)) {
                query.append(Constants.ANNOTATION, Constants.ANNOTATION_SET_NAME + "=" + annotationsetName);
            }

            QueryResult<Family> search = familyManager.search(String.valueOf(resourceIds.getStudyId()), query, new QueryOptions(),
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

    @POST
    @Path("/{family}/annotationsets/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create an annotation set for the family [DEPRECATED]", position = 13,
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
    @ApiOperation(value = "Update the annotations [DEPRECATED]", position = 15,
            notes = "Use /{family}/update instead")
    public Response updateAnnotationGET(
            @ApiParam(value = "familyId", required = true) @PathParam("family") String familyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName") String annotationsetName,
            @ApiParam(value = "JSON containing key:value annotations to update", required = true) Map<String, Object> annotations) {
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
    public Response getAcls(@ApiParam(value = "Comma separated list of family IDs or names up to a maximum of 100", required = true) @PathParam("families")
                                    String familyIdsStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false") @QueryParam("silent") boolean silent) {
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
            AclParams familyAclParams = new AclParams(params.getPermissions(), params.getAction());
            List<String> idList = getIdList(params.family);
            return createOkResponse(familyManager.updateAcl(studyStr, idList, memberId, familyAclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    protected static class MultiplesParameters {
        private String type;
        private List<String> siblings;

        public Multiples toMultiples() {
            return new Multiples(type, siblings);
        }
    }

    protected static class IndividualPOST {
        public String name;

        public String father;
        public String mother;
        public MultiplesParameters multiples;

        public Individual.Sex sex;
        public String ethnicity;
        public Boolean parentalConsanguinity;
        public Individual.Population population;
        public String dateOfBirth;
        public Individual.KaryotypicSex karyotypicSex;
        public Individual.LifeStatus lifeStatus;
        public Individual.AffectationStatus affectationStatus;
        public List<CommonModels.AnnotationSetParams> annotationSets;
        public List<OntologyTerm> phenotypes;
        public Map<String, Object> attributes;


        public Individual toIndividual(String studyStr, StudyManager studyManager, String sessionId) throws CatalogException {
            List<AnnotationSet> annotationSetList = new ArrayList<>();
            if (annotationSets != null) {
                for (CommonModels.AnnotationSetParams annotationSet : annotationSets) {
                    if (annotationSet != null) {
                        annotationSetList.add(annotationSet.toAnnotationSet(studyStr, studyManager, sessionId));
                    }
                }
            }

            return new Individual(-1, name, father != null ? new Individual().setName(father) : null,
                    mother != null ? new Individual().setName(mother) : null, multiples != null ? multiples.toMultiples() : null, sex,
                    karyotypicSex, ethnicity, population, lifeStatus, affectationStatus, dateOfBirth, null,
                    parentalConsanguinity != null ? parentalConsanguinity : false, 1, annotationSetList, phenotypes);
        }
    }

    private static class FamilyPOST {
        public String name;
        public String description;

        public List<OntologyTerm> phenotypes;
        public List<IndividualPOST> members;

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

            List<Individual> relatives = new ArrayList<>(members.size());
            for (IndividualPOST member : members) {
                relatives.add(member.toIndividual(studyStr, studyManager, sessionId));
            }

            return new Family(name, phenotypes, relatives, description, annotationSetList, attributes);
        }
    }

}
