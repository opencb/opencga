package org.opencb.opencga.server;



import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecuter;
import org.opencb.opencga.analysis.beans.InputParam;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.beans.Tool;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

@Path("/job")
///opencga/rest/v1/jobs/create?analysisId=23&tool=samtools
@Api(value = "job", description = "job")
public class JobWSServer extends OpenCGAWSServer {

    public JobWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
        params = uriInfo.getQueryParameters();
    }

    @GET
    @Path("/create")
    @Produces("application/json")
    @ApiOperation(value = "Create job")

    public Response createJob(
            @ApiParam(value = "analysisId", required = true)    @QueryParam("analysisId") int analysisId,
            @ApiParam(value = "toolId", required = true)        @QueryParam("toolId") String toolIdStr,
            @ApiParam(value = "execution", required = false)    @DefaultValue("") @QueryParam("execution") String execution,
            @ApiParam(value = "description", required = false)  @DefaultValue("") @QueryParam("description") String description
    ) {
        QueryResult<Job> jobResult;
        try {
            AnalysisJobExecuter analysisJobExecuter;
            String toolName;
            int toolId = catalogManager.getToolId(toolIdStr);
            if(toolId < 0) {
                analysisJobExecuter = new AnalysisJobExecuter(toolIdStr, execution);    //LEGACY MODE, AVOID USING
                toolName = toolIdStr;
            } else {
                Tool tool = catalogManager.getTool(toolId, sessionId).getResult().get(0);
                analysisJobExecuter = new AnalysisJobExecuter(Paths.get(tool.getPath()).getParent(), tool.getName(), execution);
                toolName = tool.getName();
            }

            List<Integer> inputFiles = new LinkedList<>();
            Map<String, List<String>> localParams = new HashMap<>(params);

            // Set input param
            for (InputParam inputParam : analysisJobExecuter.getExecution().getInputParams()) {
                if (params.containsKey(inputParam.getName())) {

                    List<String> filePaths = new LinkedList<>();
                    for (String files : params.get(inputParam.getName())) {
                        for (String fileId : files.split(",")) {
                            if (fileId.startsWith("example_")) { // is a example
                                fileId = fileId.replace("example_", "");
                                filePaths.add(analysisJobExecuter.getExamplePath(fileId));
                            } else {
                                File file = catalogManager.getFile(catalogManager.getFileId(fileId), sessionId).getResult().get(0);
                                filePaths.add(catalogManager.getFileUri(file).getPath());
                                inputFiles.add(file.getId());
                            }
                        }
                    }
                    localParams.put(inputParam.getName(), filePaths);
                }
            }

            // Creating job name. Random string to avoid collisions.
            String jobName = "J_" + String.format(StringUtils.randomString(15));

            // Get temporal outdir  TODO: Create job folder outside the user workspace.
            java.nio.file.Path temporalOutdirPath = Paths.get("jobs", jobName);
            int studyId = catalogManager.getStudyIdByAnalysisId(analysisId);
            File temporalOutDir = catalogManager.createFolder(studyId, temporalOutdirPath, true, sessionId).getResult().get(0);

            // Set outdir
            String outputParam = analysisJobExecuter.getExecution().getOutputParam();
            int outDirId = catalogManager.getFileId(params.get(outputParam).get(0));
            File outDir = catalogManager.getFile(outDirId, sessionId).getResult().get(0);

            localParams.put(outputParam, Arrays.asList(catalogManager.getFileUri(temporalOutDir).getPath()));

            // Create commandLine
            String commandLine = analysisJobExecuter.createCommandLine(localParams);
            System.out.println(commandLine);

            // Create job in CatalogManager
            jobResult = catalogManager.createJob(analysisId, jobName, toolName, description, commandLine, outDir.getId(), temporalOutDir.getId(), inputFiles, sessionId);

            // Execute job
            analysisJobExecuter.execute(jobName,temporalOutdirPath.toAbsolutePath().toString(),commandLine);

            return createOkResponse(jobResult);

        } catch (CatalogManagerException | CatalogIOManagerException | IOException | AnalysisExecutionException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    private Response executeTool(){


        return null;
    }


}