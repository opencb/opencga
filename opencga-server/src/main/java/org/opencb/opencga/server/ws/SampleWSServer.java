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

package org.opencb.opencga.server.ws;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.utils.CatalogSampleAnnotationsLoader;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jacobo on 15/12/14.
 */
@Path("/{version}/samples")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Samples", position = 6, description = "Methods for working with 'samples' endpoint")
public class SampleWSServer extends OpenCGAWSServer {


    public SampleWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                          @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @ApiOperation(value = "Create sample", position = 1)
    public Response createSample(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                 @ApiParam(value = "name", required = true) @QueryParam("name") String name,
                                 @ApiParam(value = "source", required = false) @QueryParam("source") String source,
                                 @ApiParam(value = "description", required = false) @QueryParam("description") String description) {
        try {
            QueryResult<Sample> queryResult = catalogManager.createSample(catalogManager.getStudyId(studyIdStr), name, source, description, null, null, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleId}/info")
    @ApiOperation(value = "Get sample information", position = 2)
    public Response infoSample(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") int sampleId) {
        try {
            QueryResult<Sample> queryResult = catalogManager.getSample(sampleId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/load")
    @ApiOperation(value = "Load samples from a ped file", position = 3)
    public Response loadSamples(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                @ApiParam(value = "fileId", required = false) @QueryParam("fileId") String fileIdStr,
                                @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") Integer variableSetId) {
        try {
            CatalogSampleAnnotationsLoader loader = new CatalogSampleAnnotationsLoader(catalogManager);
            File pedigreeFile = catalogManager.getFile(catalogManager.getFileId(fileIdStr), sessionId).first();
            QueryResult<Sample> sampleQueryResult = loader.loadSampleAnnotations(pedigreeFile, variableSetId, sessionId);
            return createOkResponse(sampleQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Get sample information", position = 4)
    public Response searchSamples(@ApiParam(value = "studyId", required = true) @DefaultValue("") @QueryParam("studyId") String studyIdStr,
                                  @ApiParam(value = "id") @QueryParam("id") String id,
                                  @ApiParam(value = "name") @QueryParam("name") String name,
                                  @ApiParam(value = "source") @QueryParam("source") String source,
                                  @ApiParam(value = "acl") @QueryParam("acl") String acl,
                                  @ApiParam(value = "acl.userId") @QueryParam("acl.userId") String acl_userId,
                                  @ApiParam(value = "AclEntry read permission") @QueryParam("bacl.read") String acl_read,
                                  @ApiParam(value = "AclEntry write permission") @QueryParam("bacl.write") String acl_write,
                                  @ApiParam(value = "AclEntry delete permission") @QueryParam("bacl.delete") String acl_delete,
                                  @ApiParam(value = "individualId") @QueryParam("individualId") String individualId,
                                  @ApiParam(value = "annotationSetId") @QueryParam("annotationSetId") String annotationSetId,
                                  @ApiParam(value = "variableSetId") @QueryParam("variableSetId") String variableSetId,
                                  @ApiParam(value = "annotation") @QueryParam("annotation") String annotation
                                  ) {
        try {
            QueryResult<Sample> queryResult = catalogManager.getAllSamples(catalogManager.getStudyId(studyIdStr), queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{sampleId}/annotate")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "annotate sample", position = 5)
    public Response annotateSamplePOST(@ApiParam(value = "SampleID", required = true) @PathParam("sampleId") int sampleId,
                                       @ApiParam(value = "Annotation set name. Must be unique for the sample", required = true) @QueryParam("annotateSetName") String annotateSetName,
                                       @ApiParam(value = "VariableSetId of the new annotation", required = false) @QueryParam("variableSetId") int variableSetId,
                                       @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update") @DefaultValue("false") boolean update,
                                       @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete") @DefaultValue("false") boolean delete,
                                       Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult;
            if (delete && update) {
                return createErrorResponse("Annotate sample", "Unable to update and delete annotations at the same time");
            } else if (delete) {
                queryResult = catalogManager.deleteSampleAnnotation(sampleId, annotateSetName, sessionId);
            } else if (update) {
                queryResult = catalogManager.updateSampleAnnotation(sampleId, annotateSetName, annotations, sessionId);
            } else {
                queryResult = catalogManager.annotateSample(sampleId, annotateSetName, variableSetId,
                        annotations, Collections.emptyMap(), sessionId);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleId}/annotate")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "annotate sample", position = 5)
    public Response annotateSampleGET(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") int sampleId,
                                      @ApiParam(value = "Annotation set name. Must be unique for the sample", required = true) @QueryParam("annotateSetName") String annotateSetName,
                                      @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") int variableSetId,
                                      @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update") @DefaultValue("false") boolean update,
                                      @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete") @DefaultValue("false") boolean delete) {
        try {
            QueryResult<AnnotationSet> queryResult;

            if (delete && update) {
                return createErrorResponse("Annotate sample", "Unable to update and delete annotations at the same time");
            } else if (delete) {
                queryResult = catalogManager.deleteSampleAnnotation(sampleId, annotateSetName, sessionId);
            } else {
                if (update) {
                    for (AnnotationSet annotationSet : catalogManager.getSample(sampleId, null, sessionId).first().getAnnotationSets()) {
                        if (annotationSet.getId().equals(annotateSetName)) {
                            variableSetId = annotationSet.getVariableSetId();
                        }
                    }
                }
                QueryResult<VariableSet> variableSetResult = catalogManager.getVariableSet(variableSetId, null, sessionId);
                if(variableSetResult.getResult().isEmpty()) {
                    return createErrorResponse("sample - annotate", "VariableSet not find.");
                }
                Map<String, Object> annotations = variableSetResult.getResult().get(0).getVariables().stream()
                        .filter(variable -> params.containsKey(variable.getId()))
                        .collect(Collectors.toMap(Variable::getId, variable -> params.getFirst(variable.getId())));

                if (update) {
                    queryResult = catalogManager.updateSampleAnnotation(sampleId, annotateSetName, annotations, sessionId);
                } else {
                    queryResult = catalogManager.annotateSample(sampleId, annotateSetName, variableSetId, annotations, Collections.emptyMap(), sessionId);
                }
            }

            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleId}/update")
    @ApiOperation(value = "Update some sample attributes using GET method", position = 6)
    public Response update(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") int sampleId,
                           @ApiParam(value = "description", required = true) @QueryParam("description") String description,
                           @ApiParam(value = "source", required = true) @QueryParam("source") String source,
                           @ApiParam(value = "individualId", required = true) @QueryParam("individualId") String individualId) {
        try {
            QueryResult<Sample> queryResult = catalogManager.modifySample(sampleId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class UpdateSample {
        public String description;
        public String source;
        public int individualId;
        public Map<String, Object> attributes;
    }

    @POST
    @Path("/{sampleId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some sample attributes using POST method", position = 6)
    public Response updateByPost(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") int sampleId,
                                 @ApiParam(value = "params", required = true) UpdateSample params) {
        try {
            QueryResult<Sample> queryResult = catalogManager.modifySample(sampleId, new QueryOptions(jsonObjectMapper.writeValueAsString(params)), sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleId}/share")
    @ApiOperation(value = "Update some sample attributes using GET method", position = 7)
    public Response share(@PathParam(value = "sampleId") int sampleId,
                          @ApiParam(value = "User you want to share the sample with. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("userId") String userId,
                          @ApiParam(value = "Remove the previous AclEntry", required = false) @DefaultValue("false") @QueryParam("unshare") boolean unshare,
                          @ApiParam(value = "Read permission", required = false) @DefaultValue("false") @QueryParam("read") boolean read,
                          @ApiParam(value = "Write permission", required = false) @DefaultValue("false") @QueryParam("write") boolean write,
                          @ApiParam(value = "Delete permission", required = false) @DefaultValue("false") @QueryParam("delete") boolean delete
                          /*@ApiParam(value = "Execute permission", required = false) @DefaultValue("false") @QueryParam("execute") boolean execute*/) {
        try {
            QueryResult queryResult;
            if (unshare) {
                queryResult = catalogManager.unshareSample(sampleId, userId, sessionId);
            } else {
                queryResult = catalogManager.shareSample(sampleId, new AclEntry(userId, read, write, false, delete), sessionId);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleId}/delete")
    @ApiOperation(value = "Delete a sample", position = 8)
    public Response delete(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") int sampleId) {
        try {
            QueryResult<Sample> queryResult = catalogManager.deleteSample(sampleId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
