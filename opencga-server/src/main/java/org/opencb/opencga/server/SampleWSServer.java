package org.opencb.opencga.server;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
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

    @POST
    @Path("/{sampleId}/annotate")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "annotate sample")
    public Response annotateSamplePOST(
            @ApiParam(value = "sampleId", required = true) @PathParam("sampleId") int sampleId,
            @ApiParam(value = "name", required = true) @QueryParam("name") String name,
            @ApiParam(value = "variableSetId", required = true) @QueryParam("variableSetId") int variableSetId,
            Map<String, Object> annotations
    ) {
        try {
            QueryResult<AnnotationSet> queryResult = catalogManager.annotateSample(sampleId, name, variableSetId,
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
            @ApiParam(value = "name", required = true) @QueryParam("name") String name,
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

            QueryResult<AnnotationSet> queryResult = catalogManager.annotateSample(sampleId, name, variableSetId, annotations, null, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

}
