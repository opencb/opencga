package org.opencb.opencga.server.rest.analysis;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Job;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Map;

@Path("/{apiVersion}/analysis/tool")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Tool", position = 4, description = "Methods for working with 'tool' endpoint")
public class ToolAnalysisWSService extends AnalysisWSService {

    public ToolAnalysisWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                                      @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public ToolAnalysisWSService(String apiVersion, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                                      @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(apiVersion, uriInfo, httpServletRequest, httpHeaders);
    }

    private static class ExecuteParams {
        public String jobName;
        public String description;
        @JsonProperty(required = true)
        public String toolId;
        public String execution;
        @JsonProperty(required = true)
        public String outDir;
        @JsonProperty(required = true)
        public Map<String, String> toolParams;

        public ExecuteParams() {
        }
    }

    @POST
    @Path("/execute")
    @ApiOperation(value = "Execute an analysis using an internal or external tool", response = QueryResponse.class)
    public Response run(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study")String studyStr,
            @ApiParam(value = "Json containing the execution parameters", required = true) ExecuteParams params) {
        try {
            DataResult<Job> queryResult = catalogManager.getJobManager().create(studyStr, params.jobName, params.description,
                    params.toolId, params.execution, params.outDir, params.toolParams, token);
            return createOkResponse(queryResult);
        } catch(Exception e) {
            return createErrorResponse(e);
        }
    }


}
