package org.opencb.opencga.server;


import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecuter;
import org.opencb.opencga.analysis.beans.Execution;
import org.opencb.opencga.analysis.beans.InputParam;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.IOUtils;
import org.opencb.opencga.analysis.beans.Analysis;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
            @ApiParam(value = "userId", required = true) @QueryParam("userId") String userId,
            @ApiParam(value = "projectId", required = true) @QueryParam("projectId") String projectId,
            @ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyId,
            @ApiParam(value = "analysisId", required = true) @QueryParam("analysisId") int analysisId,
            @ApiParam(value = "jobName", required = true) @QueryParam("jobName") String jobName,
            @ApiParam(value = "toolName", required = true) @QueryParam("toolName") String toolName,
            @ApiParam(value = "description", required = true) @QueryParam("description") String description
            //@ApiParam(value = "commandLine", required = true) @QueryParam("commandLine") String commandLine,
//            @ApiParam(value = "outDir", required = true) @QueryParam("outDir") String outDir,
//            @ApiParam(value = "inputFiles", required = true) @QueryParam("inputFiles") String inputFilesStr
    ) {
        QueryResult queryResult;
        try {

            String analysis = toolName;
            String analysisOwner = "system";
            AnalysisJobExecuter aje = new AnalysisJobExecuter(analysis, analysisOwner);
            Execution execution = aje.getExecution();
            int outputFileId = 7;
            boolean example = false;

            // Set input param
            List<String> dataList = new ArrayList<>();
            for (InputParam inputParam : execution.getInputParams()) {
                if (params.containsKey(inputParam.getName())) {
                    List<String> dataIds = Arrays.asList(params.get(inputParam.getName()).get(0).split(","));
                    List<String> dataPaths = new ArrayList<>();
                    for (String dataId : dataIds) {
                        String dataPath;
                        if (dataId.contains("example_")) { // is a example
                            dataId = dataId.replace("example_", "");
                            dataPath = aje.getExamplePath(dataId);
                        } else { // is a dataId
//                            dataPath = cloudSessionManager.getAccountPath(accountId).resolve(StringUtils.parseObjectId(dataId)).toString();
                            QueryResult queryFile = catalogManager.getFile(outputFileId, sessionId);
                            String relativePath = ((org.opencb.opencga.catalog.beans.File)queryFile.getResult().get(0)).getPath();
                            dataPath = catalogManager.getFileUri(userId, projectId, studyId, relativePath).toString();
                        }

                        if (dataPath.contains("ERROR")) {
                            return createErrorResponse(dataPath);
                        } else {
                            dataPaths.add(dataPath);
                            dataList.add(dataPath);
                        }
                    }
                    params.put(inputParam.getName(), dataPaths);
                }
            }



            String inputFilesStr = "5";
            String outDir = "";
            List<String> inputFilesList = Splitter.on(",").splitToList(inputFilesStr);
            List<Integer> inputFiles = new ArrayList<>();

            for(String s : inputFilesList) inputFiles.add(Integer.valueOf(s));
            String commandLine = aje.createCommandLine(execution.getExecutable(), params);
            System.out.println(commandLine);
            queryResult = catalogManager.createJob(analysisId, jobName, toolName, description, commandLine, outDir, inputFiles, sessionId);
            int jobId = ((Job)queryResult.getResult().get(0)).getId();

//            params.put("jobid", Arrays.asList(jobId+""));

            return createOkResponse(queryResult);

        } catch (CatalogManagerException | CatalogIOManagerException  e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        } catch (AnalysisExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Response executeTool(){


        return null;
    }


}