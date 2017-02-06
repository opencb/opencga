/*
 * Copyright 2015-2016 OpenCB
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
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.utils.CatalogSampleAnnotationsLoader;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 15/12/14.
 */
@Path("/{version}/samples")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Samples", position = 7, description = "Methods for working with 'samples' endpoint")
public class SampleWSServer extends OpenCGAWSServer {

    private ISampleManager sampleManager;

    public SampleWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(uriInfo, httpServletRequest);
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
    public Response infoSample(@ApiParam(value = "Comma separated list of sample IDs or names", required = true)
                                   @PathParam("samples") String sampleStr,
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

    @GET
    @Path("/create")
    @ApiOperation(value = "Create sample [WARNING]", position = 2, response = Sample.class,
    notes = "WARNING: the usage of this web service is discouraged, please use the POST version instead. Be aware that this is web service "
            + "is not tested and this can be deprecated in a future version.")
    public Response createSample(@ApiParam(value = "DEPRECATED: studyId", hidden = true) @QueryParam("studyId") String studyIdStr,
                                 @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                 @QueryParam("study") String studyStr,
                                 @ApiParam(value = "name", required = true) @QueryParam("name") String name,
                                 @ApiParam(value = "source", required = false) @QueryParam("source") String source,
                                 @ApiParam(value = "description", required = false) @QueryParam("description") String description) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            QueryResult<Sample> queryResult = sampleManager.create(studyStr, name, source, description, null, null, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private static class SampleParameters {
        public String name;
        public String source;
        public String description;
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create sample", position = 2, response = Sample.class)
    public Response createSamplePOST(
            @ApiParam(value = "DEPRECATED: studyId", hidden = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value="JSON containing sample information", required = true)  SampleParameters params) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            QueryResult<Sample> queryResult = sampleManager.create(studyStr, params.name, params.source, params.description, null, null,
                    sessionId);
            return createOkResponse(queryResult);
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
                                @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") Long variableSetId) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            if (StringUtils.isNotEmpty(fileStr)) {
                fileIdStr = fileStr;
            }
            AbstractManager.MyResourceId resourceId = catalogManager.getFileManager().getId(fileIdStr, studyStr, sessionId);

            File pedigreeFile = catalogManager.getFile(resourceId.getResourceId(), sessionId).first();
            CatalogSampleAnnotationsLoader loader = new CatalogSampleAnnotationsLoader(catalogManager);
            QueryResult<Sample> sampleQueryResult = loader.loadSampleAnnotations(pedigreeFile, variableSetId, sessionId);
            return createOkResponse(sampleQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Multi-study search that allows the user to look for files from from different studies of the same project "
            + "applying filters.", position = 4, response = Sample[].class)
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
//                                  @ApiParam(value = "acls") @QueryParam("acls") String acls,
//                                  @ApiParam(value = "acls.users") @QueryParam("acls.users") String acl_userIds,
                           @ApiParam(value = "DEPRECATED: use individual.id instead", hidden = true) @QueryParam("individualId")
                                       String individualIdOld,
                           @ApiParam(value = "Individual id or name") @QueryParam("individual.id") String individual,
                           @ApiParam(value = "Ontology terms") @QueryParam("ontologies") String ontologies,
                           @ApiParam(value = "annotationsetName") @QueryParam("annotationsetName") String annotationsetName,
                           @ApiParam(value = "variableSetId") @QueryParam("variableSetId") String variableSetId,
                           @ApiParam(value = "annotation") @QueryParam("annotation") String annotation,
                           @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount) {
        try {
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }

            // TODO: individualId is deprecated. Remember to remove this if after next release
            if (query.containsKey("individualId") && !query.containsKey(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key())) {
                query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), query.get("individualId"));
                query.remove("individualId");
            }

            QueryResult<Sample> queryResult = sampleManager.search(studyStr, query, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sample}/update")
    @ApiOperation(value = "Update some sample attributes using GET method [WARNING]", position = 6, response = Sample.class,
    notes = "WARNING: the usage of this web service is discouraged, please use the POST version instead. Be aware that this is web service "
            + "is not tested and this can be deprecated in a future version.")
    public Response update(@ApiParam(value = "Sample id or name", required = true) @PathParam("sample") String sampleStr,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                           @QueryParam("study") String studyStr,
                           @ApiParam(value = "name", required = false) @QueryParam("name") String name,
                           @ApiParam(value = "description", required = false) @QueryParam("description") String description,
                           @ApiParam(value = "source", required = false) @QueryParam("source") String source,
                           @ApiParam(value = "DEPRECATED: use individual.id instead", hidden = true) @QueryParam("individualId")
                                       String individualIdOld,
                           @ApiParam(value = "Individual id or name", required = false) @QueryParam("individual.id") String individualId,
                           @ApiParam(value = "Attributes", required = false) @QueryParam("attributes") String attributes) {
        try {
            AbstractManager.MyResourceId resourceId = catalogManager.getSampleManager().getId(sampleStr, studyStr, sessionId);

            ObjectMap params = new ObjectMap(query);
            // TODO: individualId is deprecated. Remember to remove this if after next release
            if (params.containsKey("individualId") && !params.containsKey(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key())) {
                params.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), params.get("individualId"));
                params.remove("individualId");
            }
//            params.putIfNotNull(SampleDBAdaptor.QueryParams.NAME.key(), name);
//            params.putIfNotNull(SampleDBAdaptor.QueryParams.DESCRIPTION.key(), description);
//            params.putIfNotNull(SampleDBAdaptor.QueryParams.SOURCE.key(), source);
//            params.putIfNotNull(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualId);
//            params.putIfNotNull(SampleDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);

            QueryResult<Sample> queryResult = catalogManager.getSampleManager().update(resourceId.getResourceId(), params, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class UpdateSample {
        public String name;
        public String description;
        public String source;
        @JsonProperty("individual.id")
        public String individualId;
        public Map<String, Object> attributes;
    }

    @POST
    @Path("/{sample}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some sample attributes using POST method", position = 6)
    public Response updateByPost(@ApiParam(value = "sampleId", required = true) @PathParam("sample") String sampleStr,
                                 @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                 @QueryParam("study") String studyStr,
                                 @ApiParam(value = "params", required = true) UpdateSample parameters) {
        try {
            AbstractManager.MyResourceId resourceId = catalogManager.getSampleManager().getId(sampleStr, studyStr, sessionId);

            ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(parameters));
            if (params.get("individualId") != null) {
                params.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), params.get("individualId"));
                params.remove("individualId");
            }
            QueryResult<Sample> queryResult = catalogManager.getSampleManager().update(resourceId.getResourceId(), params, queryOptions,
                    sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sample}/delete")
    @ApiOperation(value = "Delete a sample", position = 9)
    public Response delete(@ApiParam(value = "Comma separated list of sample IDs or names", required = true) @PathParam("sample")
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
                            @ApiParam(value = "DEPRECATED: use indiviudal.id instead", hidden = true) @QueryParam("individualId")
                                        String individualIdOld,
                            @ApiParam(value = "Individual id or name") @QueryParam("individual.id") String individualId,
                            @ApiParam(value = "annotationsetName") @QueryParam("annotationsetName") String annotationsetName,
                            @ApiParam(value = "variableSetId") @QueryParam("variableSetId") String variableSetId,
                            @ApiParam(value = "annotation") @QueryParam("annotation") String annotation) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }

            // TODO: individualId is deprecated. Remember to remove this if after next release
            if (query.containsKey("individualId") && !query.containsKey(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key())) {
                query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), query.get("individualId"));
                query.remove("individualId");
            }
            QueryResult result = sampleManager.groupBy(studyStr, query, queryOptions, fields, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sample}/annotationsets/search")
    @ApiOperation(value = "Search annotation sets [NOT TESTED]", position = 11)
    public Response searchAnnotationSetGET(@ApiParam(value = "sampleId", required = true) @PathParam("sample") String sampleStr,
                                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                           @QueryParam("study") String studyStr,
                                           @ApiParam(value = "variableSetId") @QueryParam("variableSetId") long variableSetId,
                                           @ApiParam(value = "annotation") @QueryParam("annotation") String annotation,
                                           @ApiParam(value = "Indicates whether to show the annotations as key-value",
                                                   defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(sampleManager.searchAnnotationSetAsMap(sampleStr, studyStr, variableSetId, annotation, sessionId));
            } else {
                return createOkResponse(sampleManager.searchAnnotationSet(sampleStr, studyStr, variableSetId, annotation, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sample}/annotationsets/info")
    @ApiOperation(value = "Return the annotation sets of the sample [NOT TESTED]", position = 12)
    public Response infoAnnotationSetGET(@ApiParam(value = "sampleId", required = true) @PathParam("sample") String sampleStr,
                                         @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                         @QueryParam("study") String studyStr,
                                         @ApiParam(value = "Indicates whether to show the annotations as key-value",
                                                 defaultValue = "false") @QueryParam("asMap") boolean asMap) {
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

    @POST
    @Path("/{sample}/annotationsets/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create an annotation set for the sample [NOT TESTED]", position = 13)
    public Response annotateSamplePOST(
            @ApiParam(value = "SampleId", required = true) @PathParam("sample") String sampleStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "VariableSetId of the new annotation", required = true) @QueryParam("variableSetId") long variableSetId,
            @ApiParam(value="JSON containing the annotation set name and the array of annotations. The name should be unique for the "
                    + "sample", required = true) CohortWSServer.AnnotationsetParameters params) {
        try {
            QueryResult<AnnotationSet> queryResult = sampleManager.createAnnotationSet(sampleStr, studyStr, variableSetId, params.name,
                    params.annotations, Collections.emptyMap(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sample}/annotationsets/{annotationsetName}/delete")
    @ApiOperation(value = "Delete the annotation set or the annotations within the annotation set [NOT TESTED]", position = 14)
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
    @ApiOperation(value = "Update the annotations [NOT TESTED]", position = 15)
    public Response updateAnnotationGET(@ApiParam(value = "sampleId", required = true) @PathParam("sample") String sampleIdStr,
                                        @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                        @QueryParam("study") String studyStr,
                                        @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName") String annotationsetName,
//                                        @ApiParam(value = "reset", required = false) @QueryParam("reset") String reset,
                                        Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult = sampleManager.updateAnnotationSet(sampleIdStr, studyStr, annotationsetName,
                    annotations, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sample}/annotationsets/{annotationsetName}/info")
    @ApiOperation(value = "Return the annotation set [NOT TESTED]", position = 16)
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

    @GET
    @Path("/{samples}/acl")
    @ApiOperation(value = "Returns the acl of the samples", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of sample IDs or names", required = true) @PathParam("samples")
                                    String sampleIdsStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr) {
        try {
            return createOkResponse(catalogManager.getAllSampleAcls(sampleIdsStr, studyStr, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{samples}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of members", hidden = true, position = 19)
    public Response createRole(@ApiParam(value = "Comma separated list of sample IDs or names", required = true) @PathParam("samples")
                                           String sampleIdsStr,
                               @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                               @QueryParam("study") String studyStr,
                               @ApiParam(value = "Comma separated list of permissions that will be granted to the member list")
                                   @DefaultValue("") @QueryParam("permissions") String permissions,
                               @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true)
                                   @DefaultValue("") @QueryParam("members") String members) {
        try {
            return createOkResponse(catalogManager.createSampleAcls(sampleIdsStr, studyStr, members, permissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{samples}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of members", position = 19)
    public Response createRolePOST(
            @ApiParam(value = "Comma separated list of sample IDs or names", required = true) @PathParam("samples") String sampleIdsStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value="JSON containing the parameters defined in GET. Mandatory keys: 'members'", required = true)
                    StudyWSServer.CreateAclCommands params) {
        try {
            return createOkResponse(catalogManager.createSampleAcls(sampleIdsStr, studyStr, params.members, params.permissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sample}/acl/{memberId}/info")
    @ApiOperation(value = "Returns the set of permissions granted for the member", position = 20)
    public Response getAcl(@ApiParam(value = "Sample id or name", required = true) @PathParam("sample") String sampleIdStr,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                           @QueryParam("study") String studyStr,
                           @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.getSampleAcl(sampleIdStr, studyStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sample}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", hidden = true, position = 21)
    public Response updateAcl(@ApiParam(value = "Sample id or name", required = true) @PathParam("sample") String sampleIdStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                              @QueryParam("study") String studyStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
                              @ApiParam(value = "Comma separated list of permissions to add", required = false) @QueryParam("add") String addPermissions,
                              @ApiParam(value = "Comma separated list of permissions to remove", required = false) @QueryParam("remove") String removePermissions,
                              @ApiParam(value = "Comma separated list of permissions to set", required = false) @QueryParam("set") String setPermissions) {
        try {
            return createOkResponse(catalogManager.updateSampleAcl(sampleIdStr, studyStr, memberId, addPermissions, removePermissions,
                    setPermissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{sample}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAclPOST(
            @ApiParam(value = "Sample id or name", required = true) @PathParam("sample") String sampleIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value="JSON containing one of the keys 'add', 'set' or 'remove'", required = true)
                    StudyWSServer.MemberAclUpdate params) {
        try {
            return createOkResponse(catalogManager.updateSampleAcl(sampleIdStr, studyStr, memberId, params.add, params.remove, params.set,
                    sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{samples}/acl/{memberId}/delete")
    @ApiOperation(value = "Remove all the permissions granted for the member", position = 22)
    public Response deleteAcl(@ApiParam(value = "Comma separated list of sample IDs or names", required = true) @PathParam("samples") String sampleIdsStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                              @QueryParam("study") String studyStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.removeSampleAcl(sampleIdsStr, studyStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
