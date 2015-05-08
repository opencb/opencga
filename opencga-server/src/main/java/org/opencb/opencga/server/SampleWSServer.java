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

package org.opencb.opencga.server;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogSampleAnnotationsLoader;
import org.opencb.opencga.catalog.beans.*;

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
@Path("/samples")
@Api(value = "samples", description = "Samples")
public class SampleWSServer extends OpenCGAWSServer {

    public SampleWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
        params = uriInfo.getQueryParameters();
    }

    @GET
    @Path("/create")
    @Produces("application/json")
    @ApiOperation(value = "Create sample")
    public Response createSample(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                 @ApiParam(value = "name", required = true) @QueryParam("name") String name,
                                 @ApiParam(value = "source", required = false) @QueryParam("source") String source,
                                 @ApiParam(value = "description", required = false) @QueryParam("description") String description) {
        try {
            QueryResult<Sample> queryResult = catalogManager.createSample(catalogManager.getStudyId(studyIdStr), name, source, description, null, null, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/load")
    @Produces("application/json")
    @ApiOperation(value = "Load samples from a ped file")
    public Response loadSamples(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                @ApiParam(value = "fileId", required = false) @QueryParam("fileId") String fileIdStr,
                                @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") Integer variableSetId) {
        try {
            CatalogSampleAnnotationsLoader loader = new CatalogSampleAnnotationsLoader(catalogManager);
            File pedigreeFile = catalogManager.getFile(catalogManager.getFileId(fileIdStr), sessionId).first();
            QueryResult<Sample> sampleQueryResult = loader.loadSampleAnnotations(pedigreeFile, variableSetId, sessionId);
            return createOkResponse(sampleQueryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{sampleId}/info")
    @Produces("application/json")
    @ApiOperation(value = "Get sample information")
    public Response infoSample(@ApiParam(value = "sampleId", required = true) @PathParam("sampleId") int sampleId) {
        try {
            QueryResult<Sample> queryResult = catalogManager.getSample(sampleId, this.getQueryOptions(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/search")
    @Produces("application/json")
    @ApiOperation(value = "Get sample information")
    public Response searchSamples(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr) {
        try {

            QueryOptions queryOptions = getAllQueryOptions();
            QueryResult<Sample> queryResult = catalogManager.getAllSamples(catalogManager.getStudyId(studyIdStr), queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @POST
    @Path("/{sampleId}/annotate")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "annotate sample")
    public Response annotateSamplePOST(
            @ApiParam(value = "sampleId", required = true) @PathParam("sampleId") int sampleId,
            @ApiParam(value = "id", required = true) @QueryParam("id") String id,
            @ApiParam(value = "variableSetId", required = true) @QueryParam("variableSetId") int variableSetId,
            Map<String, Object> annotations
    ) {
        try {
            QueryResult<AnnotationSet> queryResult = catalogManager.annotateSample(sampleId, id, variableSetId,
                    annotations, this.getQueryOptions(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{sampleId}/annotate")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "annotate sample")
    public Response annotateSampleGET(
            @ApiParam(value = "sampleId", required = true) @PathParam("sampleId") int sampleId,
            @ApiParam(value = "id", required = true) @QueryParam("id") String id,
            @ApiParam(value = "variableSetId", required = true) @QueryParam("variableSetId") int variableSetId
    ) {
        try {

            QueryResult<VariableSet> variableSetResult = catalogManager.getVariableSet(variableSetId, null, sessionId);
            if(variableSetResult.getResult().isEmpty()) {
                return createErrorResponse("VariableSet not find.");
            }
            Map<String, Object> annotations = new HashMap<>();
            for (Variable variable : variableSetResult.getResult().get(0).getVariables()) {
                if(params.containsKey(variable.getId())) {
                    annotations.put(variable.getId(), params.getFirst(variable.getId()));
                }
            }

            QueryResult<AnnotationSet> queryResult = catalogManager.annotateSample(sampleId, id, variableSetId, annotations, this.getQueryOptions(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

}
