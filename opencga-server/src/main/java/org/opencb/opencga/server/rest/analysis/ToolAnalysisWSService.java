package org.opencb.opencga.server.rest.analysis;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Map;

@Path("/{version}/analysis/tool")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Tool", position = 4, description = "Methods for working with 'tool' endpoint")
public class ToolAnalysisWSService extends AnalysisWSService {

    public ToolAnalysisWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                                      @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public ToolAnalysisWSService(String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                                      @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
    }

    @POST
    @Path("/run")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Run analysis", response = QueryResponse.class)
    public Response run(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Job name. If not provided, the name will be equal to toolId_currentDate") @QueryParam("name") String name,
            @ApiParam(value = "Job description") @QueryParam("description") String description,
            @ApiParam(value = "Tool id (opencga-analysis, vaast, samtools...)", required = true) @QueryParam("toolId") String toolId,
            @ApiParam(value = "Execution: '(samtools) view'") @QueryParam("execution") String execution,
            @ApiParam(value = "File path to store the results", required = true) @QueryParam("outDir") String outDir,
            @ApiParam(value = "Tool parameters", name = "params", required = true) Map<String, String> params) {
        try {
            QueryResult<Job> queryResult = catalogManager.getJobManager().create(studyStr, name, description, toolId, execution, outDir,
                    params, sessionId);
            return createOkResponse(queryResult);
        } catch(Exception e) {
            return createErrorResponse(e);
        }
    }


}
