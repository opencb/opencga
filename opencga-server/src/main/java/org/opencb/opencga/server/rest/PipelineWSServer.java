package org.opencb.opencga.server.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.opencb.opencga.catalog.managers.PipelineManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.job.Pipeline;
import org.opencb.opencga.core.models.job.PipelineCreateParams;
import org.opencb.opencga.core.response.OpenCGAResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

@Path("/{apiVersion}/pipelines")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Pipelines", description = "Methods for working with 'pipelines' endpoint")
public class PipelineWSServer extends OpenCGAWSServer {

    private PipelineManager pipelineManager;

    public PipelineWSServer(UriInfo uriInfo, HttpServletRequest httpServletRequest, HttpHeaders httpHeaders)
            throws VersionException, IOException {
        super(uriInfo, httpServletRequest, httpHeaders);
        pipelineManager = catalogManager.getPipelineManager();
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Register a new Pipeline", response = Pipeline.class)
    public Response createPipeline(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "body", required = true) PipelineCreateParams pipeline) {
        try {
            OpenCGAResult<Pipeline> result = pipelineManager.create(studyStr, pipeline.toPipeline(), queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
