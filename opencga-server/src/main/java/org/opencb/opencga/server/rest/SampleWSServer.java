/*
 * Copyright 2015 OpenCB
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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
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


    public SampleWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @ApiOperation(value = "Create sample", position = 1, response = Sample.class)
    public Response createSample(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                 @ApiParam(value = "name", required = true) @QueryParam("name") String name,
                                 @ApiParam(value = "source", required = false) @QueryParam("source") String source,
                                 @ApiParam(value = "description", required = false) @QueryParam("description") String description) {
        try {
            long studyId = catalogManager.getStudyId(studyIdStr, sessionId);
            QueryResult<Sample> queryResult = catalogManager.createSample(studyId, name, source, description, null, null, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleIds}/info")
    @ApiOperation(value = "Get sample information", position = 2, response = Sample.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response infoSample(@ApiParam(value = "Comma separated list of sample ids or names", required = true) @PathParam("sampleIds") String sampleStr) {
        try {
            try {
                List<QueryResult<Sample>> queryResults = new LinkedList<>();
                List<Long> sampleIds = catalogManager.getSampleIds(sampleStr, sessionId);
                for (Long sampleId : sampleIds) {
                    queryResults.add(catalogManager.getSample(sampleId, queryOptions, sessionId));
                }
                return createOkResponse(queryResults);
            } catch (Exception e) {
                return createErrorResponse(e);
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/load")
    @ApiOperation(value = "Load samples from a ped file", position = 3)
    public Response loadSamples(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                @ApiParam(value = "fileId", required = false) @QueryParam("fileId") String fileIdStr,
                                @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") Long variableSetId) {
        try {
            long fileId = catalogManager.getFileId(fileIdStr, sessionId);
            CatalogSampleAnnotationsLoader loader = new CatalogSampleAnnotationsLoader(catalogManager);
            File pedigreeFile = catalogManager.getFile(fileId, sessionId).first();
            QueryResult<Sample> sampleQueryResult = loader.loadSampleAnnotations(pedigreeFile, variableSetId, sessionId);
            return createOkResponse(sampleQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Get sample information", position = 4, response = Sample[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response searchSamples(@ApiParam(value = "studyId", required = true) @DefaultValue("") @QueryParam("studyId") String studyIdStr,
                                  @ApiParam(value = "id") @QueryParam("id") String id,
                                  @ApiParam(value = "name") @QueryParam("name") String name,
                                  @ApiParam(value = "source") @QueryParam("source") String source,
//                                  @ApiParam(value = "acls") @QueryParam("acls") String acls,
//                                  @ApiParam(value = "acls.users") @QueryParam("acls.users") String acl_userIds,
                                  @ApiParam(value = "individualId") @QueryParam("individualId") String individualId,
                                  @ApiParam(value = "annotationSetName") @QueryParam("annotationSetName") String annotationSetName,
                                  @ApiParam(value = "variableSetId") @QueryParam("variableSetId") String variableSetId,
                                  @ApiParam(value = "annotation") @QueryParam("annotation") String annotation
                                  ) {
        try {
            long studyId = catalogManager.getStudyId(studyIdStr, sessionId);
            QueryOptions qOptions = new QueryOptions();
            parseQueryParams(params, SampleDBAdaptor.QueryParams::getParam, query, qOptions);
            QueryResult<Sample> queryResult = catalogManager.getAllSamples(studyId, query, qOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @POST
//    @Path("/{sampleId}/annotate")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "annotate sample", position = 5)
//    public Response annotateSamplePOST(@ApiParam(value = "SampleID", required = true) @PathParam("sampleId") long sampleId,
//                                       @ApiParam(value = "Annotation set name. Must be unique for the sample", required = true) @QueryParam("annotateSetName") String annotateSetName,
//                                       @ApiParam(value = "VariableSetId of the new annotation", required = false) @QueryParam("variableSetId") long variableSetId,
//                                       @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update") @DefaultValue("false") boolean update,
//                                       @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete") @DefaultValue("false") boolean delete,
//                                       Map<String, Object> annotations) {
//        try {
//            QueryResult<AnnotationSet> queryResult;
//            if (delete && update) {
//                return createErrorResponse("Annotate sample", "Unable to update and delete annotations at the same time");
//            } else if (delete) {
//                queryResult = catalogManager.deleteSampleAnnotation(sampleId, annotateSetName, sessionId);
//            } else if (update) {
//                queryResult = catalogManager.updateSampleAnnotation(sampleId, annotateSetName, annotations, sessionId);
//            } else {
//                queryResult = catalogManager.annotateSample(sampleId, annotateSetName, variableSetId,
//                        annotations, Collections.emptyMap(), sessionId);
//            }
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

//    @GET
//    @Path("/{sampleId}/annotate")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "annotate sample", position = 5)
//    public Response annotateSampleGET(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") long sampleId,
//                                      @ApiParam(value = "Annotation set name. Must be unique for the sample", required = true) @QueryParam("annotateSetName") String annotateSetName,
//                                      @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") long variableSetId,
//                                      @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update") @DefaultValue("false") boolean update,
//                                      @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete") @DefaultValue("false") boolean delete) {
//        try {
//            QueryResult<AnnotationSet> queryResult;
//
//            if (delete && update) {
//                return createErrorResponse("Annotate sample", "Unable to update and delete annotations at the same time");
//            } else if (delete) {
//                queryResult = catalogManager.deleteSampleAnnotation(sampleId, annotateSetName, sessionId);
//            } else {
//                if (update) {
//                    for (AnnotationSet annotationSet : catalogManager.getSample(sampleId, null, sessionId).first().getAnnotationSets()) {
//                        if (annotationSet.getId().equals(annotateSetName)) {
//                            variableSetId = annotationSet.getVariableSetId();
//                        }
//                    }
//                }
//                QueryResult<VariableSet> variableSetResult = catalogManager.getVariableSet(variableSetId, null, sessionId);
//                if(variableSetResult.getResult().isEmpty()) {
//                    return createErrorResponse("sample - annotate", "VariableSet not find.");
//                }
//                Map<String, Object> annotations = variableSetResult.getResult().get(0).getVariables().stream()
//                        .filter(variable -> params.containsKey(variable.getId()))
//                        .collect(Collectors.toMap(Variable::getId, variable -> params.getFirst(variable.getId())));
//
//                if (update) {
//                    queryResult = catalogManager.updateSampleAnnotation(sampleId, annotateSetName, annotations, sessionId);
//                } else {
//                    queryResult = catalogManager.annotateSample(sampleId, annotateSetName, variableSetId, annotations, Collections.emptyMap(), sessionId);
//                }
//            }
//
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @GET
    @Path("/{sampleId}/update")
    @ApiOperation(value = "Update some sample attributes using GET method", position = 6, response = Sample.class)
    public Response update(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") String sampleStr,
                           @ApiParam(value = "name", required = false) @QueryParam("name") String name,
                           @ApiParam(value = "description", required = false) @QueryParam("description") String description,
                           @ApiParam(value = "source", required = false) @QueryParam("source") String source,
                           @ApiParam(value = "individualId", required = false) @QueryParam("individualId") String individualId) {
        try {
            // FIXME: The id resolution should not go here
            long sampleId = catalogManager.getSampleId(sampleStr, sessionId);
            QueryResult<Sample> queryResult = catalogManager.modifySample(sampleId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class UpdateSample {
        public String name;
        public String description;
        public String source;
        public long individualId;
        public Map<String, Object> attributes;
    }

    @POST
    @Path("/{sampleId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some sample attributes using POST method", position = 6)
    public Response updateByPost(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") String sampleStr,
                                 @ApiParam(value = "params", required = true) UpdateSample params) {
        try {
            // FIXME: The id resolution should not go here
            long sampleId = catalogManager.getSampleId(sampleStr, sessionId);
            QueryResult<Sample> queryResult = catalogManager.modifySample(sampleId,
                    new QueryOptions(jsonObjectMapper.writeValueAsString(params)), sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
//
//    @GET
//    @Path("/{sampleIds}/share")
//    @ApiOperation(value = "Share samples with other members", position = 7)
//    public Response share(@PathParam(value = "sampleIds") String sampleIds,
//                          @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members,
//                          @ApiParam(value = "Comma separated list of sample permissions", required = false) @DefaultValue("") @QueryParam("acls") String acls,
//                          @ApiParam(value = "Boolean indicating whether to allow the change of permissions in case any member already had any", required = true) @DefaultValue("false") @QueryParam("override") boolean override) {
//        try {
//            return createOkResponse(catalogManager.shareSample(sampleIds, members, Arrays.asList(acls.split(",")), override,
//                    sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    @GET
//    @Path("/{sampleIds}/unshare")
//    @ApiOperation(value = "Remove the permissions for the list of members", position = 8)
//    public Response unshare(@PathParam(value = "sampleIds") String sampleIds,
//                            @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members,
//                            @ApiParam(value = "Comma separated list of sample permissions", required = false) @DefaultValue("") @QueryParam("acls") String acls) {
//        try {
//            return createOkResponse(catalogManager.unshareSample(sampleIds, members, acls, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @GET
    @Path("/{sampleId}/delete")
    @ApiOperation(value = "Delete a sample", position = 9)
    public Response delete(@ApiParam(value = "Comma separated list of sample ids", required = true) @PathParam("sampleId") String sampleStr) {
        try {
            List<QueryResult<Sample>> delete = catalogManager.getSampleManager().delete(sampleStr, queryOptions, sessionId);
            return createOkResponse(delete);
        } catch (CatalogException | IOException e) {
            return createErrorResponse(e);
        }
//        try {
//            // FIXME: The id resolution should not go here
//            long sampleId = catalogManager.getSampleId(sampleStr, sessionId);
//            QueryResult<Sample> queryResult = catalogManager.deleteSample(sampleId, queryOptions, sessionId);
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group samples by several fields", position = 10)
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("") @QueryParam("fields") String fields,
                            @ApiParam(value = "studyId", required = true) @DefaultValue("") @QueryParam("studyId") String studyIdStr,
                            @ApiParam(value = "Comma separated list of ids.") @QueryParam("id") String id,
                            @ApiParam(value = "Comma separated list of names.") @QueryParam("name") String name,
                            @ApiParam(value = "source") @QueryParam("source") String source,
                            @ApiParam(value = "individualId") @QueryParam("individualId") String individualId,
                            @ApiParam(value = "annotationSetName") @QueryParam("annotationSetName") String annotationSetName,
                            @ApiParam(value = "variableSetId") @QueryParam("variableSetId") String variableSetId,
                            @ApiParam(value = "annotation") @QueryParam("annotation") String annotation) {
        try {
            Query query = new Query();
            QueryOptions qOptions = new QueryOptions();
            parseQueryParams(params, FileDBAdaptor.QueryParams::getParam, query, qOptions);

            logger.debug("query = " + query.toJson());
            logger.debug("queryOptions = " + qOptions.toJson());
            QueryResult result = catalogManager.sampleGroupBy(query, qOptions, fields, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleId}/annotationSets/search")
    @ApiOperation(value = "Search annotation sets [NOT TESTED]", position = 11)
    public Response searchAnnotationSetGET(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") String sampleStr,
                                           @ApiParam(value = "variableSetId") @QueryParam("variableSetId") long variableSetId,
                                           @ApiParam(value = "annotation") @QueryParam("annotation") String annotation,
                                           @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "true") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(catalogManager.getSampleManager().searchAnnotationSetAsMap(sampleStr, variableSetId, annotation, sessionId));
            } else {
                return createOkResponse(catalogManager.getSampleManager().searchAnnotationSet(sampleStr, variableSetId, annotation, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleId}/annotationSets/info")
    @ApiOperation(value = "Return the annotation sets of the sample [NOT TESTED]", position = 12)
    public Response infoAnnotationSetGET(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") String sampleStr,
                                         @ApiParam(value = "Indicates whether to show the annotations as key-value", required = false, defaultValue = "true") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(catalogManager.getSampleManager().getAllAnnotationSetsAsMap(sampleStr, sessionId));
            } else {
                return createOkResponse(catalogManager.getSampleManager().getAllAnnotationSets(sampleStr, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{sampleId}/annotationSets/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create an annotation set for the sample [NOT TESTED]", position = 13)
    public Response annotateSamplePOST(@ApiParam(value = "SampleId", required = true) @PathParam("sampleId") String sampleStr,
                                       @ApiParam(value = "VariableSetId of the new annotation", required = true) @QueryParam("variableSetId") long variableSetId,
                                       @ApiParam(value = "Annotation set name. Must be unique for the sample", required = true) @QueryParam("annotateSetName") String annotateSetName,
                                       Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult = catalogManager.createSampleAnnotationSet(sampleStr, variableSetId, annotateSetName,
                    annotations, Collections.emptyMap(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleId}/annotationSets/{annotationSetName}/delete")
    @ApiOperation(value = "Delete the annotation set or the annotations within the annotation set [NOT TESTED]", position = 14)
    public Response deleteAnnotationGET(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") String sampleStr,
                                        @ApiParam(value = "annotationSetName", required = true) @PathParam("annotationSetName") String annotationSetName,
                                        @ApiParam(value = "[NOT IMPLEMENTED] Comma separated list of annotation names to be deleted", required = false) @QueryParam("annotations") String annotations) {
        try {
            QueryResult<AnnotationSet> queryResult;
            if (annotations != null) {
                queryResult = catalogManager.deleteSampleAnnotations(sampleStr, annotationSetName, annotations, sessionId);
            } else {
                queryResult = catalogManager.deleteSampleAnnotationSet(sampleStr, annotationSetName, sessionId);
            }
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{sampleId}/annotationSets/{annotationSetName}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update the annotations [NOT TESTED]", position = 15)
    public Response updateAnnotationGET(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") String sampleIdStr,
                                        @ApiParam(value = "annotationSetName", required = true) @PathParam("annotationSetName") String annotationSetName,
//                                        @ApiParam(value = "reset", required = false) @QueryParam("reset") String reset,
                                        Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult = catalogManager.updateSampleAnnotationSet(sampleIdStr, annotationSetName,
                    annotations, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleId}/annotationSets/{annotationSetName}/info")
    @ApiOperation(value = "Return the annotation set [NOT TESTED]", position = 16)
    public Response infoAnnotationGET(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") String sampleStr,
                                        @ApiParam(value = "annotationSetName", required = true) @PathParam("annotationSetName") String annotationSetName,
                                        @ApiParam(value = "Indicates whether to show the annotations as key-value", required = false, defaultValue = "true") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(catalogManager.getSampleManager().getAnnotationSetAsMap(sampleStr, annotationSetName, sessionId));
            } else {
                return createOkResponse(catalogManager.getSampleManager().getAnnotationSet(sampleStr, annotationSetName, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }



    @GET
    @Path("/{sampleIds}/acl")
    @ApiOperation(value = "Returns the acl of the samples", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of sample ids", required = true) @PathParam("sampleIds") String sampleIdsStr) {
        try {
            return createOkResponse(catalogManager.getAllSampleAcls(sampleIdsStr, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{sampleIds}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of members", position = 19)
    public Response createRole(@ApiParam(value = "Comma separated list of sample ids", required = true) @PathParam("sampleIds") String sampleIdsStr,
                               @ApiParam(value = "Comma separated list of permissions that will be granted to the member list", required = false) @DefaultValue("") @QueryParam("permissions") String permissions,
                               @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members) {
        try {
            return createOkResponse(catalogManager.createSampleAcls(sampleIdsStr, members, permissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleId}/acl/{memberId}/info")
    @ApiOperation(value = "Returns the set of permissions granted for the member", position = 20)
    public Response getAcl(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") String sampleIdStr,
                           @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.getSampleAcl(sampleIdStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleId}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") String sampleIdStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
                              @ApiParam(value = "Comma separated list of permissions to add", required = false) @QueryParam("addPermissions") String addPermissions,
                              @ApiParam(value = "Comma separated list of permissions to remove", required = false) @QueryParam("removePermissions") String removePermissions,
                              @ApiParam(value = "Comma separated list of permissions to set", required = false) @QueryParam("setPermissions") String setPermissions) {
        try {
            return createOkResponse(catalogManager.updateSampleAcl(sampleIdStr, memberId, addPermissions, removePermissions, setPermissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleIds}/acl/{memberId}/delete")
    @ApiOperation(value = "Remove all the permissions granted for the member", position = 22)
    public Response deleteAcl(@ApiParam(value = "Comma separated list of sample ids", required = true) @PathParam("sampleIds") String sampleIdsStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.removeSampleAcl(sampleIdsStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
