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
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Tool;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jacobo on 30/10/14.
 */
@Path("/{version}/tools")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Tools", position = 8, description = "Methods for working with 'tools' endpoint")
public class ToolWSServer extends OpenCGAWSServer {


    public ToolWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                        @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/{toolId}/info")
    @ApiOperation(value = "Tool info")
    public Response info(@PathParam(value = "toolId") @DefaultValue("") @FormDataParam("toolId") String toolId,
                         @ApiParam(value = "execution", required = false)  @DefaultValue("") @QueryParam("execution") String execution) {
        String[] toolIds = toolId.split(",");
        try {
            List<QueryResult> results = new LinkedList<>();
            for (String id : toolIds) {
                QueryResult<Tool> toolResult = catalogManager.getTool(catalogManager.getToolId(id), sessionId);
                Tool tool = toolResult.getResult().get(0);
                AnalysisJobExecutor analysisJobExecutor = new AnalysisJobExecutor(Paths.get(tool.getPath()).getParent(), tool.getName(), execution);
                tool.setManifest(analysisJobExecutor.getAnalysis());
                tool.setResult(analysisJobExecutor.getResult());
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
    @ApiOperation(value = "Tool help")
    public Response help(@PathParam(value = "toolId") @DefaultValue("") @FormDataParam("toolId") String toolId,
                         @ApiParam(value = "execution", required = false)  @DefaultValue("") @QueryParam("execution") String execution) {
        String[] toolIds = toolId.split(",");
        try {
            List<String> results = new LinkedList<>();
            for (String id : toolIds) {
                Tool tool = catalogManager.getTool(catalogManager.getToolId(id), sessionId).getResult().get(0);
                AnalysisJobExecutor analysisJobExecutor = new AnalysisJobExecutor(Paths.get(tool.getPath()).getParent(), tool.getName(), execution);
                String help = analysisJobExecutor.help("");
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
