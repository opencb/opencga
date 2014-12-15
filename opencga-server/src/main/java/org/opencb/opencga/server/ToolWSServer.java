package org.opencb.opencga.server;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecuter;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.beans.Tool;
import org.opencb.opencga.catalog.db.CatalogDBException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jacobo on 30/10/14.
 */
@Path("/tools")
@Api(value = "tools", description = "tools", position = 6)
public class ToolWSServer extends OpenCGAWSServer{

    public ToolWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/{toolId}/info")
    @Produces("application/json")
    @ApiOperation(value = "Tool info")
    public Response info(@PathParam(value = "toolId") @DefaultValue("") @FormDataParam("toolId") String toolId,
                         @ApiParam(value = "execution", required = false)  @DefaultValue("") @QueryParam("execution") String execution
    ) {
        String[] splitedToolId = toolId.split(",");
        try {
            List<QueryResult> results = new LinkedList<>();
            for (String id : splitedToolId) {
                QueryResult<Tool> toolResult = catalogManager.getTool(catalogManager.getToolId(id), sessionId);
                Tool tool = toolResult.getResult().get(0);
                AnalysisJobExecuter analysisJobExecuter = new AnalysisJobExecuter(Paths.get(tool.getPath()).getParent(), tool.getName(), execution);
                tool.setManifest(analysisJobExecuter.getAnalysis());
                tool.setResult(analysisJobExecuter.getResult());
                results.add(toolResult);
            }
            return createOkResponse(results);
        } catch (CatalogException | AnalysisExecutionException | IOException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{toolId}/help")
    @Produces("application/json")
    @ApiOperation(value = "Tool help")
    public Response help(@PathParam(value = "toolId") @DefaultValue("") @FormDataParam("toolId") String toolId,
                         @ApiParam(value = "execution", required = false)  @DefaultValue("") @QueryParam("execution") String execution
    ) {
        String[] splitedToolId = toolId.split(",");
        try {
            List<String> results = new LinkedList<>();
            for (String id : splitedToolId) {
                Tool tool = catalogManager.getTool(catalogManager.getToolId(id), sessionId).getResult().get(0);
                AnalysisJobExecuter analysisJobExecuter = new AnalysisJobExecuter(Paths.get(tool.getPath()).getParent(), tool.getName(), execution);
                String help = analysisJobExecuter.help("");
                System.out.println(help);
                results.add(help);
            }
            return createOkResponse(results);
        } catch (CatalogException | AnalysisExecutionException | IOException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

}
