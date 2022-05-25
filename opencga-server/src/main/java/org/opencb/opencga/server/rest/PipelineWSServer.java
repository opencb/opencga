package org.opencb.opencga.server.rest;

import org.opencb.opencga.catalog.managers.PipelineManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.job.Pipeline;
import org.opencb.opencga.core.models.job.PipelineCreateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Api;
import org.opencb.opencga.core.tools.annotations.ApiOperation;
import org.opencb.opencga.core.tools.annotations.ApiParam;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;

@Path("/{apiVersion}/pipelines")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Pipelines", description = "Methods for working with 'pipelines' endpoint")
public class PipelineWSServer extends OpenCGAWSServer {

    private PipelineManager pipelineManager;

    public PipelineWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
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
