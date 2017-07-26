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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.api.ISampleManager;
import org.opencb.opencga.catalog.managers.api.IStudyManager;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.OntologyTerm;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.utils.CatalogSampleAnnotationsLoader;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;

/**
 * Created by jacobo on 15/12/14.
 */
@Path("/{version}/samples")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Samples", position = 7, description = "Methods for working with 'samples' endpoint")
public class SampleWSServer extends OpenCGAWSServer {

    private ISampleManager sampleManager;

    public SampleWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        sampleManager = catalogManager.getSampleManager();
    }

    @GET
    @Path("/{samples}/info")
    @ApiOperation(value = "Get sample information", position = 1, response = Sample.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "lazy", value = "False to return the entire individual object", defaultValue = "true", dataType = "boolean", paramType = "query")
    })
    public Response infoSample(
            @ApiParam(value = "Comma separated list of sample IDs or names", required = true) @PathParam("samples") String sampleStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr) {
        try {
            AbstractManager.MyResourceIds resourceIds = sampleManager.getIds(sampleStr, studyStr, sessionId);

            List<QueryResult<Sample>> queryResults = new LinkedList<>();
            if (resourceIds.getResourceIds() != null && resourceIds.getResourceIds().size() > 0) {
                for (Long sampleId : resourceIds.getResourceIds()) {
                    queryResults.add(catalogManager.getSample(sampleId, queryOptions, sessionId));
                }
            }
            return createOkResponse(queryResults);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create sample", position = 2, response = Sample.class)
    public Response createSamplePOST(
            @ApiParam(value = "DEPRECATED: studyId", hidden = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value="JSON containing sample information", required = true) CreateSamplePOST params) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }

            return createOkResponse(sampleManager.create(studyStr, params.toSample(studyStr, catalogManager.getStudyManager(), sessionId),
                    queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/load")
    @ApiOperation(value = "Load samples from a ped file", position = 3)
    public Response loadSamples(@ApiParam(value = "DEPRECATED: studyId", hidden = true) @QueryParam("studyId") String studyIdStr,
                                @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr,
                                @ApiParam(value = "DEPRECATED: use file instead", hidden = true) @QueryParam("fileId") String fileIdStr,
                                @ApiParam(value = "file", required = true) @QueryParam("file") String fileStr,
                                @ApiParam(value = "variableSetId", hidden = true) @QueryParam("variableSetId") Long variableSetId,
                                @ApiParam(value = "variableSet", required = false) @QueryParam("variableSet") String variableSet) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            if (StringUtils.isNotEmpty(fileStr)) {
                fileIdStr = fileStr;
            }
            if (variableSetId != null) {
                variableSet = Long.toString(variableSetId);
            }
            AbstractManager.MyResourceId resourceId = catalogManager.getFileManager().getId(fileIdStr, studyStr, sessionId);
            long varSetId = catalogManager.getStudyManager().getVariableSetId(variableSet, studyStr, sessionId).getResourceId();

            File pedigreeFile = catalogManager.getFile(resourceId.getResourceId(), sessionId).first();
            CatalogSampleAnnotationsLoader loader = new CatalogSampleAnnotationsLoader(catalogManager);
            QueryResult<Sample> sampleQueryResult = loader.loadSampleAnnotations(pedigreeFile, varSetId, sessionId);
            return createOkResponse(sampleQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Sample search method", position = 4, response = Sample[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = "lazy", value = "False to return the entire individual object", defaultValue = "true", dataType = "boolean", paramType = "query")
    })
    public Response search(@ApiParam(value = "DEPRECATED: use study instead", hidden = true) @QueryParam("studyId") String studyIdStr,
                           @ApiParam(value = "Study [[user@]project:]{study1,study2|*}  where studies and project can be either the id or"
                                          + " alias.", required = false) @QueryParam("study") String studyStr,
                           @ApiParam(value = "DEPRECATED: use /info instead", hidden = true) @QueryParam("id") String id,
                           @ApiParam(value = "name") @QueryParam("name") String name,
                           @ApiParam(value = "source") @QueryParam("source") String source,
                           @ApiParam(value = "type") @QueryParam("type") String type,
                           @ApiParam(value = "somatic") @QueryParam("somatic") Boolean somatic,
                           @ApiParam(value = "Individual id or name", hidden = true) @QueryParam("individual.id") String individualId,
                           @ApiParam(value = "Individual id or name") @QueryParam("individual") String individual,
                           @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
                           @ApiParam(value = "Ontology terms") @QueryParam("ontologies") String ontologies,
                           @ApiParam(value = "annotationsetName") @QueryParam("annotationsetName") String annotationsetName,
                           @ApiParam(value = "variableSetId", hidden = true) @QueryParam("variableSetId") String variableSetId,
                           @ApiParam(value = "variableSet") @QueryParam("variableSet") String variableSet,
                           @ApiParam(value = "Annotation, e.g: key1=value(,key2=value)") @QueryParam("annotation") String annotation,
                           @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount,
                           @ApiParam(value = "Release value") @QueryParam("release") String release) {
        try {
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }

            // TODO: individualId is deprecated. Remember to remove this if after next release
            if (query.containsKey(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key())) {
                if (!query.containsKey(SampleDBAdaptor.QueryParams.INDIVIDUAL.key())) {
                    query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), query.get(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key()));
                }
                query.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key());
            }

            QueryResult<Sample> queryResult;
            if (count) {
                queryResult = sampleManager.count(studyStr, query, sessionId);
            } else {
                queryResult = sampleManager.search(studyStr, query, queryOptions, sessionId);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{sample}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some sample attributes using POST method", position = 6)
    public Response updateByPost(@ApiParam(value = "sampleId", required = true) @PathParam("sample") String sampleStr,
                                 @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                 @QueryParam("study") String studyStr,
                                 @ApiParam(value = "params", required = true) UpdateSamplePOST parameters) {
        try {
            AbstractManager.MyResourceId resourceId = catalogManager.getSampleManager().getId(sampleStr, studyStr, sessionId);

            ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(parameters));
            if (params.containsKey(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key())) {
                if (!params.containsKey(SampleDBAdaptor.QueryParams.INDIVIDUAL.key())) {
                    params.put(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), params.get(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key()));
                }
                params.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key());
            }

            if (params.size() == 0) {
                throw new CatalogException("Missing parameters to update.");
            }

            QueryResult<Sample> queryResult = catalogManager.getSampleManager().update(resourceId.getResourceId(), params, queryOptions,
                    sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{samples}/delete")
    @ApiOperation(value = "Delete a sample [NOT TESTED]", position = 9)
    public Response delete(@ApiParam(value = "Comma separated list of sample IDs or names", required = true) @PathParam("samples")
                                       String sampleStr,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                           @QueryParam("study") String studyStr) {
        try {
            List<QueryResult<Sample>> delete = catalogManager.getSampleManager().delete(sampleStr, studyStr, queryOptions, sessionId);
            return createOkResponse(delete);
        } catch (CatalogException | IOException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group samples by several fields", position = 10)
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
                            @QueryParam("fields") String fields,
                            @ApiParam(value = "DEPRECATED: use study instead", hidden = true) @DefaultValue("") @QueryParam("studyId")
                                    String studyIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr,
                            @ApiParam(value = "DEPRECATED: Comma separated list of ids.", hidden = true) @QueryParam("id") String id,
                            @ApiParam(value = "Comma separated list of names.") @QueryParam("name") String name,
                            @ApiParam(value = "source") @QueryParam("source") String source,
                            @ApiParam(value = "Individual id or name", hidden = true) @QueryParam("individual.id") String individualId,
                            @ApiParam(value = "Individual id or name") @QueryParam("individual") String individual,
                            @ApiParam(value = "annotationsetName") @QueryParam("annotationsetName") String annotationsetName,
                            @ApiParam(value = "variableSetId", hidden = true) @QueryParam("variableSetId") String variableSetId,
                            @ApiParam(value = "variableSet") @QueryParam("variableSet") String variableSet,
                            @ApiParam(value = "Annotation, e.g: key1=value(,key2=value)") @QueryParam("annotation") String annotation) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }

            // TODO: individualId is deprecated. Remember to remove this if after next release
            if (query.containsKey(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key())) {
                if (!query.containsKey(SampleDBAdaptor.QueryParams.INDIVIDUAL.key())) {
                    query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), query.get(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key()));
                }
                query.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key());
            }
            QueryResult result = sampleManager.groupBy(studyStr, query, queryOptions, fields, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sample}/annotationsets/search")
    @ApiOperation(value = "Search annotation sets", position = 11)
    public Response searchAnnotationSetGET(
            @ApiParam(value = "sampleId", required = true) @PathParam("sample") String sampleStr,
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
                return createOkResponse(sampleManager.searchAnnotationSetAsMap(sampleStr, studyStr, variableSet, annotation, sessionId));
            } else {
                return createOkResponse(sampleManager.searchAnnotationSet(sampleStr, studyStr, variableSet, annotation, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sample}/annotationsets")
    @ApiOperation(value = "Return the annotation sets of the sample", position = 12)
    public Response getAnnotationSet(
            @ApiParam(value = "Sample id or name", required = true) @PathParam("sample") String sampleStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap,
            @ApiParam(value = "Annotation set name. If provided, only chosen annotation set will be shown") @QueryParam("name") String annotationsetName) {
        try {
            if (asMap) {
                if (StringUtils.isNotEmpty(annotationsetName)) {
                    return createOkResponse(sampleManager.getAnnotationSetAsMap(sampleStr, studyStr, annotationsetName, sessionId));
                } else {
                    return createOkResponse(sampleManager.getAllAnnotationSetsAsMap(sampleStr, studyStr, sessionId));
                }
            } else {
                if (StringUtils.isNotEmpty(annotationsetName)) {
                    return createOkResponse(sampleManager.getAnnotationSet(sampleStr, studyStr, annotationsetName, sessionId));
                } else {
                    return createOkResponse(sampleManager.getAllAnnotationSets(sampleStr, studyStr, sessionId));
                }
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sample}/annotationsets/info")
    @ApiOperation(value = "Return the annotation sets of the sample [DEPRECATED]", position = 12,
            notes = "Use /{sample}/annotationsets instead")
    public Response infoAnnotationSetGET(
            @ApiParam(value = "Sample id or name", required = true) @PathParam("sample") String sampleStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(sampleManager.getAllAnnotationSetsAsMap(sampleStr, studyStr, sessionId));
            } else {
                return createOkResponse(sampleManager.getAllAnnotationSets(sampleStr, studyStr, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{sample}/annotationsets/{annotationsetName}/info")
    @ApiOperation(value = "Return the annotation set [DEPRECATED]", position = 16, notes = "Use /{sample}/annotationsets/info instead")
    public Response infoAnnotationGET(@ApiParam(value = "sampleId", required = true) @PathParam("sample") String sampleStr,
                                      @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                      @QueryParam("study") String studyStr,
                                      @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName") String annotationsetName,
                                      @ApiParam(value = "Indicates whether to show the annotations as key-value",
                                              defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(catalogManager.getSampleManager().getAnnotationSetAsMap(sampleStr, studyStr, annotationsetName,
                        sessionId));
            } else {
                return createOkResponse(catalogManager.getSampleManager().getAnnotationSet(sampleStr, studyStr, annotationsetName,
                        sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{sample}/annotationsets/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create an annotation set for the sample", position = 13)
    public Response annotateSamplePOST(
            @ApiParam(value = "SampleId", required = true) @PathParam("sample") String sampleStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Variable set id or name", hidden = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = "Variable set id or name", required = true) @QueryParam("variableSet") String variableSet,
            @ApiParam(value="JSON containing the annotation set name and the array of annotations. The name should be unique for the "
                    + "sample", required = true) CohortWSServer.AnnotationsetParameters params) {
        try {
            if (StringUtils.isNotEmpty(variableSetId)) {
                variableSet = variableSetId;
            }
            QueryResult<AnnotationSet> queryResult = sampleManager.createAnnotationSet(sampleStr, studyStr, variableSet, params.name,
                    params.annotations, Collections.emptyMap(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sample}/annotationsets/{annotationsetName}/delete")
    @ApiOperation(value = "Delete the annotation set or the annotations within the annotation set", position = 14)
    public Response deleteAnnotationGET(@ApiParam(value = "sampleId", required = true) @PathParam("sample") String sampleStr,
                                        @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                        @QueryParam("study") String studyStr,
                                        @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName") String annotationsetName,
                                        @ApiParam(value = "[NOT IMPLEMENTED] Comma separated list of annotation names to be deleted", required = false) @QueryParam("annotations") String annotations) {
        try {
            QueryResult<AnnotationSet> queryResult;
            if (annotations != null) {
                queryResult = sampleManager.deleteAnnotations(sampleStr, studyStr, annotationsetName, annotations, sessionId);
            } else {
                queryResult = sampleManager.deleteAnnotationSet(sampleStr, studyStr, annotationsetName, sessionId);
            }
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{sample}/annotationsets/{annotationsetName}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update the annotations", position = 15)
    public Response updateAnnotationGET(
            @ApiParam(value = "sampleId", required = true) @PathParam("sample") String sampleIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName") String annotationsetName,
            @ApiParam(value="JSON containing key:value annotations to update", required = true) Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult = sampleManager.updateAnnotationSet(sampleIdStr, studyStr, annotationsetName,
                    annotations, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{samples}/acl")
    @ApiOperation(value = "Returns the acl of the samples. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of sample IDs or names", required = true) @PathParam("samples")
                                    String sampleIdsStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member) {
        try {
            if (StringUtils.isEmpty(member)) {
                return createOkResponse(catalogManager.getAllSampleAcls(sampleIdsStr, studyStr, sessionId));
            } else {
                return createOkResponse(catalogManager.getSampleAcl(sampleIdsStr, studyStr, member, sessionId));
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{sample}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member [DEPRECATED]", position = 21,
            notes = "DEPRECATED: The usage of this webservice is discouraged. A different entrypoint /acl/{members}/update has been added "
                    + "to also support changing permissions using queries.")
    public Response updateAclPOST(
            @ApiParam(value = "Sample id or name", required = true) @PathParam("sample") String sampleIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value="JSON containing one of the keys 'add', 'set' or 'remove'", required = true) StudyWSServer.MemberAclUpdateOld params) {
        try {
            Sample.SampleAclParams sampleAclParams = getAclParams(params.add, params.remove, params.set);
            return createOkResponse(sampleManager.updateAcl(sampleIdStr, studyStr, memberId, sampleAclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    // Temporal method used by deprecated methods. This will be removed at some point.
    @Override
    protected Sample.SampleAclParams getAclParams(@ApiParam(value = "Comma separated list of permissions to add", required = false)
                                                      @QueryParam("add") String addPermissions,
                                                @ApiParam(value = "Comma separated list of permissions to remove", required = false)
                                                      @QueryParam("remove") String removePermissions,
                                                @ApiParam(value = "Comma separated list of permissions to set", required = false)
                                                      @QueryParam("set") String setPermissions) throws CatalogException {
        int count = 0;
        count += StringUtils.isNotEmpty(setPermissions) ? 1 : 0;
        count += StringUtils.isNotEmpty(addPermissions) ? 1 : 0;
        count += StringUtils.isNotEmpty(removePermissions) ? 1 : 0;
        if (count > 1) {
            throw new CatalogException("Only one of add, remove or set parameters are allowed.");
        } else if (count == 0) {
            throw new CatalogException("One of add, remove or set parameters is expected.");
        }

        String permissions = null;
        AclParams.Action action = null;
        if (StringUtils.isNotEmpty(addPermissions)) {
            permissions = addPermissions;
            action = AclParams.Action.ADD;
        }
        if (StringUtils.isNotEmpty(setPermissions)) {
            permissions = setPermissions;
            action = AclParams.Action.SET;
        }
        if (StringUtils.isNotEmpty(removePermissions)) {
            permissions = removePermissions;
            action = AclParams.Action.REMOVE;
        }
        return new Sample.SampleAclParams(permissions, action, null, null, null);
    }

    public static class SampleAcl extends AclParams {
        public String sample;
        public String individual;
        public String file;
        public String cohort;

        public boolean propagate;
    }

    @POST
    @Path("/acl/{memberIds}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("memberIds") String memberId,
            @ApiParam(value="JSON containing the parameters to update the permissions. If propagate flag is set to true, it will "
                    + "propagate the permissions defined to the individuals that are associated to the matching samples", required = true)
                    SampleAcl params) {
        try {
            Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams(
                    params.getPermissions(), params.getAction(), params.individual, params.file, params.cohort, params.propagate);
            return createOkResponse(sampleManager.updateAcl(params.sample, studyStr, memberId, sampleAclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private static class SamplePOST {
        public String name;
        public String description;
        public String type;
        public String source;
        public boolean somatic;
        public List<OntologyTerm> ontologyTerms;
        public List<CommonModels.AnnotationSetParams> annotationSets;
        public Map<String, Object> attributes;
    }

    public static class UpdateSamplePOST extends SamplePOST {
        @JsonProperty("individual.id")
        public String individualId;
        public String individual;
    }

    private static class CreateSamplePOST extends SamplePOST {
        public IndividualWSServer.IndividualPOST individual;

        public Sample toSample(String studyStr, IStudyManager studyManager, String sessionId) throws CatalogException {
            List<AnnotationSet> annotationSetList = new ArrayList<>();
            if (annotationSets != null) {
                for (CommonModels.AnnotationSetParams annotationSet : annotationSets) {
                    if (annotationSet != null) {
                        annotationSetList.add(annotationSet.toAnnotationSet(studyStr, studyManager, sessionId));
                    }
                }
            }

            return new Sample(-1, name, source, individual != null ? individual.toIndividual(studyStr, studyManager, sessionId) : null,
                    description, type, somatic, 1, null, annotationSetList, ontologyTerms, attributes);
        }
    }
}
