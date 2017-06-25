package org.opencb.opencga.server.rest.analysis;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Map;

/**
 * Created by pfurio on 02/06/17.
 */
@Path("/{version}/analysis/tool")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Tool", position = 4, description = "Methods to work with external tools")
public class ToolWService extends AnalysisWSService {
/*
* public JobWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        jobManager = catalogManager.getJobManager();
    }
* */
    public ToolWService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public ToolWService(String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                        @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
    }

    @POST
    @Path("/{toolId}/run")
    @ApiOperation(value = "Create a job to run an external tool", position = 23, response = Job.class)
    public Response run(
            @ApiParam(value = "Tool id") @PathParam(value = "toolId") String toolId,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value="JSON containing sample information", required = true) ToolRunParameters params) {
        try {
            return createOkResponse(catalogManager.getJobManager().create(studyStr, params.jobName, toolId, params.executionId,
                    params.params, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private static class ToolRunParameters {
        public String executionId;
        public String jobName;
        public Map<String, String> params;
    }

}
