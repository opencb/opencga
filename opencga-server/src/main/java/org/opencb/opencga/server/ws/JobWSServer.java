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
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecutor;
import org.opencb.opencga.analysis.beans.Execution;
import org.opencb.opencga.analysis.beans.InputParam;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Tool;
import org.opencb.opencga.core.common.TimeUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

///opencga/rest/v1/jobs/create?analysisId=23&tool=samtools
@Path("/{version}/jobs")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Jobs", position = 5, description = "Methods for working with 'jobs' endpoint")
public class JobWSServer extends OpenCGAWSServer {


    public JobWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                       @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
    }

//    @GET
//    @Path("/search")
//    @Produces("application/json")
//    @ApiOperation(value = "Search jobs")
//
//    public Response search(
//            @ApiParam(value = "analysisId", required = true)    @DefaultValue("-1") @QueryParam("analysisId") int analysisId,
//    ) {
//        catalogManager.search
//    }

    @GET
    @Path("/create")
    @ApiOperation(value = "Create job", position = 1)
    public Response createJob(
//            @ApiParam(value = "analysisId", required = true)    @DefaultValue("-1") @QueryParam("analysisId") int analysisId,
            @ApiParam(value = "name", required = true) @DefaultValue("") @QueryParam("name") String name,
            @ApiParam(value = "studyId", required = true) @DefaultValue("-1") @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "toolId", required = true) @DefaultValue("") @QueryParam("toolId") String toolIdStr,
            @ApiParam(value = "execution", required = false) @DefaultValue("") @QueryParam("execution") String execution,
            @ApiParam(value = "description", required = false) @DefaultValue("") @QueryParam("description") String description
    ) {
        QueryResult<Job> jobResult;
        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
            AnalysisJobExecutor analysisJobExecutor;
            String toolName;
            int toolId = catalogManager.getToolId(toolIdStr);
            if (toolId < 0) {
                analysisJobExecutor = new AnalysisJobExecutor(toolIdStr, execution);    //LEGACY MODE, AVOID USING
                toolName = toolIdStr;
            } else {
                Tool tool = catalogManager.getTool(toolId, sessionId).getResult().get(0);
                analysisJobExecutor = new AnalysisJobExecutor(Paths.get(tool.getPath()).getParent(), tool.getName(), execution);
                toolName = tool.getName();
            }

            List<Integer> inputFiles = new LinkedList<>();
            Map<String, List<String>> localParams = new HashMap<>(params);

            Execution ex = analysisJobExecutor.getExecution();
            // Set input param
            for (InputParam inputParam : ex.getInputParams()) {
                if (params.containsKey(inputParam.getName())) {

                    List<String> filePaths = new LinkedList<>();
                    for (String files : params.get(inputParam.getName())) {
                        for (String fileId : files.split(",")) {
                            if (fileId.startsWith("example_")) { // is a example
                                fileId = fileId.replace("example_", "");
                                filePaths.add(analysisJobExecutor.getExamplePath(fileId));
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
//            String jobName = name.isEmpty()? "J_" + String.format(StringUtils.randomString(15)) : name;

            // Get temporal outdir  TODO: Create job folder outside the user workspace.
//            java.nio.file.Path temporalOutdirPath = Paths.get("jobs", jobName);
////            int studyId = catalogManager.getStudyIdByAnalysisId(studyId);
//            File temporalOutDir = catalogManager.createFolder(studyId, temporalOutdirPath, true, sessionId).getResult().get(0);

            // Set outdir
            String outputParam = analysisJobExecutor.getExecution().getOutputParam();
            if (params.get(outputParam).isEmpty()) {
                return createErrorResponse("Missing output param '" + outputParam + "'");
            }

            int outDirId;
//            System.out.println("outputParam = " + outputParam);
            if(params.get(outputParam).get(0).equalsIgnoreCase("analysis")){
                QueryOptions query = new QueryOptions();
                query.put("name", params.get(outputParam).get(0));
                QueryResult<File> result = catalogManager.searchFile(studyId, query, this.getQueryOptions(), sessionId);
                outDirId = result.getResult().get(0).getId();
            }
            else
                outDirId = catalogManager.getFileId(params.get(outputParam).get(0));
            System.out.println("entrooo4");
            File outDir = catalogManager.getFile(outDirId, sessionId).getResult().get(0);


            //create job folder with timestamp to store job result files
            boolean parents = true;
            java.nio.file.Path jobOutDirPath = Paths.get(outDir.getPath(), TimeUtils.getTime());
            QueryResult<File> queryResult = catalogManager.createFolder(studyId, jobOutDirPath, parents, getQueryOptions(), sessionId);
            File jobOutDir = queryResult.getResult().get(0);


            //create input files from text - inputParamsFromTxt
            if (ex.getInputParamsFromTxt() != null) {
                for (InputParam inputParam : ex.getInputParamsFromTxt()) {
                    java.nio.file.Path relativeFilePath = Paths.get(jobOutDir.getPath(), inputParam.getName());
//                    List<String> paramInputName = params.get(inputParam.getName());
                    List<String> queryParam = params.get(inputParam.getName());
                    if (queryParam != null && queryParam.size() > 0) {

                        String value = queryParam.get(0).replace(",",System.getProperty("line.separator"));
                        QueryResult<File> createdFileResult = catalogManager.createFile(studyId, File.Format.PLAIN , File.Bioformat.NONE,
                                relativeFilePath.toString(), value.getBytes(),  "", true, sessionId);
                        File createdFile = createdFileResult.getResult().get(0);

                        queryParam.set(0, catalogManager.getFileUri(createdFile).getPath());
                        //the "-text" suffix param will be removed to replace the input parameter, so -text param content will be mandatory over the non -text parameter.
                        localParams.put(inputParam.getName().replace("-text", ""), queryParam);
                    }
                }
            }


            // Create temporal Outdir
//            String randomString = StringUtils.randomString(10);
//            URI temporalOutDirUri = catalogManager.createJobOutDir(studyId, randomString, sessionId);
//            localParams.put(outputParam, Arrays.asList(temporalOutDirUri.getPath()));
//
//            // Create commandLine
//            String commandLine = analysisJobExecuter.createCommandLine(localParams);
//            System.out.println(commandLine);
//
//            // Create job in CatalogManager
//            Map<String, Object> resourceManagerAttributes = new HashMap<>();
//            resourceManagerAttributes.put(Job.JOB_SCHEDULER_NAME, randomString);
//
//            jobResult = catalogManager.createJob(studyId, jobName, toolName, description, commandLine, temporalOutDirUri,
//                    outDir.getId(), inputFiles, resourceManagerAttributes, sessionId);
//            Job job = jobResult.getResult().get(0);

            QueryResult<Job> jobQueryResult = analysisJobExecutor.createJob(
                    localParams, catalogManager, studyId, name, description, jobOutDir, inputFiles, sessionId);

            // Execute job
//            analysisJobExecuter.execute(jobName, job.getId(), temporalOutDirUri.getPath(), commandLine);
//            AnalysisJobExecutor.execute(jobQueryResult.getResult().get(0));
            //Job will be executed by the Daemon. status: PREPARED

            return createOkResponse(jobQueryResult);

        } catch (CatalogException | IOException | AnalysisExecutionException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{jobId}/info")
    @ApiOperation(value = "Get job information", position = 2)
    public Response info(@ApiParam(value = "jobId", required = true) @PathParam("jobId") int jobId) {
        try {
            return createOkResponse(catalogManager.getJob(jobId, this.getQueryOptions(), sessionId));
        } catch (CatalogException e) {
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{jobId}/visit")
    @ApiOperation(value = "Increment job visits", position = 3)
    public Response visit(@ApiParam(value = "jobId", required = true) @PathParam("jobId") int jobId) {
        try {
            return createOkResponse(catalogManager.incJobVisites(jobId, sessionId));
        } catch (CatalogException e) {
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{jobId}/delete")
    @ApiOperation(value = "Delete job", position = 4)
    public Response delete(@ApiParam(value = "jobId", required = true) @PathParam("jobId") int jobId,
                           @ApiParam(value = "deleteFiles", required = true) @DefaultValue("true") @QueryParam("deleteFiles") boolean deleteFiles) {
        List<QueryResult> results = new LinkedList<>();
        try {
            if (deleteFiles) {
                QueryResult<Job> jobQueryResult = catalogManager.getJob(jobId, null, sessionId);
                for (Integer fileId : jobQueryResult.getResult().get(0).getOutput()) {
                    QueryResult queryResult = catalogManager.deleteFile(fileId, sessionId);
                    results.add(queryResult);
                }
            }
            results.add(catalogManager.deleteJob(jobId, sessionId));
            return createOkResponse(results);
        } catch (CatalogException | IOException e) {
            return createErrorResponse(e.getMessage());
        }
    }

}