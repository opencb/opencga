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
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
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
    public Response searchSamples(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr) {
        try {
//            QueryOptions queryOptions = getAllQueryOptions();
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
    public Response annotateSamplePOST(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") int sampleId,
                                       @ApiParam(value = "id", required = true) @QueryParam("id") String id,
                                       @ApiParam(value = "variableSetId", required = true) @QueryParam("variableSetId") int variableSetId,
                                       Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult = catalogManager.annotateSample(sampleId, id, variableSetId,
                    annotations, queryOptions, sessionId);
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
                                      @ApiParam(value = "id", required = true) @QueryParam("id") String id,
                                      @ApiParam(value = "variableSetId", required = true) @QueryParam("variableSetId") int variableSetId) {
        try {
            QueryResult<VariableSet> variableSetResult = catalogManager.getVariableSet(variableSetId, null, sessionId);
            if(variableSetResult.getResult().isEmpty()) {
                return createErrorResponse("sample - annotate", "VariableSet not find.");
            }
            Map<String, Object> annotations = new HashMap<>();
            variableSetResult.getResult().get(0).getVariables().stream()
                    .filter(variable -> params.containsKey(variable.getId()))
                    .forEach(variable -> {
                        annotations.put(variable.getId(), params.getFirst(variable.getId()));
                    });
            QueryResult<AnnotationSet> queryResult = catalogManager.annotateSample(sampleId, id, variableSetId, annotations, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{sampleId}/update")
    @ApiOperation(value = "Update some user attributes using GET method", position = 6)
    public Response update(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") String sampleId) throws IOException {
        return createErrorResponse("update - GET", "PENDING");
    }

    @POST
    @Path("/{sampleId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some user attributes using POST method", position = 6)
    public Response updateByPost(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") String sampleId,
                                 @ApiParam(value = "params", required = true) Map<String, Object> params) {
        return createErrorResponse("upodate - POST", "PENDING");
    }

    @GET
    @Path("/{sampleId}/delete")
    @ApiOperation(value = "Delete an user [NO TESTED]", position = 7)
    public Response delete(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") String sampleId) {
        return createErrorResponse("delete", "PENDING");
    }

}
