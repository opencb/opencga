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

package org.opencb.opencga.server.rest;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiModelProperty;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.analysis.ToolManager;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.beans.Execution;
import org.opencb.opencga.analysis.beans.InputParam;
import org.opencb.opencga.analysis.JobFactory;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Tool;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exception.VersionException;

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
                       @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest);
    }


    public static class InputJob {
        public InputJob() {
        }

        public InputJob(String name, String toolName, String description, long startTime, long endTime, String commandLine, Status status, long outDirId,
                        List<Long> input, Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes) {
            this.name = name;
            this.toolName = toolName;
            this.description = description;
            this.startTime = startTime;
            this.endTime = endTime;
            this.commandLine = commandLine;
            this.status = status;
            this.outDirId = outDirId;
            this.input = input;
            this.attributes = attributes;
            this.resourceManagerAttributes = resourceManagerAttributes;
        }

        enum Status{READY, ERROR}
        @ApiModelProperty(required = true) 
        public String name;
        @ApiModelProperty(required = true) 
        public String toolName;
        public String description;
        public String execution;
        public Map<String, String> params;
        public long startTime;
        public long endTime;
        @ApiModelProperty(required = true)
        public String commandLine;
        public Status status = Status.READY;
        public String statusMessage;
        @ApiModelProperty(required = true)
        public long outDirId;
        public List<Long> input;
        public List<Long> output;
        public Map<String, Object> attributes;
        public Map<String, Object> resourceManagerAttributes;

    }

    // TODO: Change the name for register. We are not "creating" a job, meaning that it will be put into execution, we are just registering
    // TODO: it, so it would be necessary changing the path name "create" per "register"
    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Register an executed job with POST method", position = 1,
            notes = "Registers a job that has been previously run outside catalog into catalog. <br>"
                    + "Required values: [name, toolName, commandLine]")
    public Response createJobPOST(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                  @ApiParam(value = "studies", required = true) InputJob job) {
        try {
            long studyId = catalogManager.getStudyId(studyIdStr);
            Job.JobStatus jobStatus;
            if (Job.JobStatus.isValid(job.status.toString())) {
                jobStatus = new Job.JobStatus(job.status.toString(), job.statusMessage);
            } else {
                jobStatus = new Job.JobStatus();
                jobStatus.setMessage(job.statusMessage);
            }
            QueryResult<Job> result = catalogManager.createJob(studyId, job.name, job.toolName, job.description, job.execution, job.params,
                    job.commandLine, null, job.outDirId, job.input, job.output, job.attributes, job.resourceManagerAttributes, jobStatus,
                    job.startTime, job.endTime, queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/create")
    @ApiOperation(value = "Create job", position = 1)
    public Response createJob(
//            @ApiParam(defaultValue = "analysisId", required = true)    @DefaultValue("-1") @QueryParam("analysisId") int analysisId,
            @ApiParam(value = "name", required = true) @DefaultValue("") @QueryParam("name") String name,
            @ApiParam(value = "studyId", required = true) @DefaultValue("-1") @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "toolId", required = true) @DefaultValue("") @QueryParam("toolId") String toolIdStr,
            @ApiParam(value = "execution", required = false) @DefaultValue("") @QueryParam("execution") String execution,
            @ApiParam(value = "description", required = false) @DefaultValue("") @QueryParam("description") String description
    ) {
        QueryResult<Job> jobResult;
        try {
            long studyId = catalogManager.getStudyId(studyIdStr);
            ToolManager toolManager;
            JobFactory jobFactory = new JobFactory(catalogManager);
            String toolName;
            long toolId = catalogManager.getToolId(toolIdStr);
            if (toolId < 0) {
                toolManager = new ToolManager(toolIdStr, execution);    //LEGACY MODE, AVOID USING
                toolName = toolIdStr;
            } else {
                Tool tool = catalogManager.getTool(toolId, sessionId).getResult().get(0);
                toolManager = new ToolManager(Paths.get(tool.getPath()).getParent(), tool.getName(), execution);
                toolName = tool.getName();
            }

            List<Long> inputFiles = new LinkedList<>();
            Map<String, List<String>> localParams = new HashMap<>(params);

            Execution ex = toolManager.getExecution();
            // Set input param
            for (InputParam inputParam : ex.getInputParams()) {
                if (params.containsKey(inputParam.getName())) {

                    List<String> filePaths = new LinkedList<>();
                    for (String files : params.get(inputParam.getName())) {
                        for (String fileId : files.split(",")) {
                            if (fileId.startsWith("example_")) { // is a example
                                fileId = fileId.replace("example_", "");
                                filePaths.add(toolManager.getExamplePath(fileId));
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
            String outputParam = toolManager.getExecution().getOutputParam();
            if (params.get(outputParam).isEmpty()) {
                return createErrorResponse("", "Missing output param '" + outputParam + "'");
            }

            long outDirId;
//            System.out.println("outputParam = " + outputParam);
            if(params.get(outputParam).get(0).equalsIgnoreCase("analysis")){
                QueryOptions query = new QueryOptions();
                query.put("name", params.get(outputParam).get(0));
                QueryResult<File> result = catalogManager.searchFile(studyId, new Query(query), queryOptions, sessionId);
                outDirId = result.getResult().get(0).getId();
            }
            else
                outDirId = catalogManager.getFileId(params.get(outputParam).get(0));
            System.out.println("entrooo4");
            File outDir = catalogManager.getFile(outDirId, sessionId).getResult().get(0);


            //create job folder with timestamp to store job result files
            boolean parents = true;
            java.nio.file.Path jobOutDirPath = Paths.get(outDir.getPath(), TimeUtils.getTime());
            QueryResult<File> queryResult = catalogManager.createFolder(studyId, jobOutDirPath, parents, queryOptions, sessionId);
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


            QueryResult<Job> jobQueryResult = jobFactory.createJob(toolManager, localParams, studyId, name,
                    description, jobOutDir, inputFiles, sessionId);

            return createOkResponse(jobQueryResult);

        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{jobId}/info")
    @ApiOperation(value = "Get job information", position = 2)
    public Response info(@ApiParam(value = "jobId", required = true) @PathParam("jobId") long jobId) {
        try {
            return createOkResponse(catalogManager.getJob(jobId, queryOptions, sessionId));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "File info", position = 12)
    public Response search(@ApiParam(value = "id", required = false) @DefaultValue("") @QueryParam("id") String id,
                           @ApiParam(value = "studyId", required = true) @DefaultValue("") @QueryParam("studyId") String studyId,
                           @ApiParam(value = "name", required = false) @DefaultValue("") @QueryParam("name") String name,
                           @ApiParam(value = "path", required = false) @DefaultValue("") @QueryParam("path") String path,
                           @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") File.FileStatus status,
                           @ApiParam(value = "ownerId", required = false) @DefaultValue("") @QueryParam("ownerId") String ownerId,
                           @ApiParam(value = "creationDate", required = false) @DefaultValue("") @QueryParam("creationDate") String creationDate,
                           @ApiParam(value = "modificationDate", required = false) @DefaultValue("") @QueryParam("modificationDate") String modificationDate,
                           @ApiParam(value = "description", required = false) @DefaultValue("") @QueryParam("description") String description,
                           @ApiParam(value = "jobId", required = false) @DefaultValue("") @QueryParam("jobId") String jobId,
                           @ApiParam(value = "attributes", required = false) @DefaultValue("") @QueryParam("attributes") String attributes,
                           @ApiParam(value = "numerical attributes", required = false) @DefaultValue("") @QueryParam("nattributes") String nattributes) {
        try {
            long studyIdNum = catalogManager.getStudyId(studyId);
            // TODO this must be changed: only one queryOptions need to be passed
            Query query = new Query();
            QueryOptions qOptions = new QueryOptions(this.queryOptions);
            parseQueryParams(params, CatalogJobDBAdaptor.QueryParams::getParam, query, qOptions);

            if (query.containsKey(CatalogJobDBAdaptor.QueryParams.NAME.key())
                    && (query.get(CatalogJobDBAdaptor.QueryParams.NAME.key()) == null
                    || query.getString(CatalogJobDBAdaptor.QueryParams.NAME.key()).isEmpty())) {
                query.remove(CatalogJobDBAdaptor.QueryParams.NAME.key());
                logger.debug("Name attribute empty, it's been removed");
            }

            if (!qOptions.containsKey(MongoDBCollection.LIMIT)) {
                qOptions.put(MongoDBCollection.LIMIT, 1000);
                logger.debug("Adding a limit of 1000");
            }
            logger.debug("query = " + query.toJson());
            QueryResult<Job> result = catalogManager.getAllJobs(studyIdNum, query, qOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{jobId}/visit")
    @ApiOperation(value = "Increment job visits", position = 3)
    public Response visit(@ApiParam(value = "jobId", required = true) @PathParam("jobId") long jobId) {
        try {
            return createOkResponse(catalogManager.incJobVisites(jobId, sessionId));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{jobId}/delete")
    @ApiOperation(value = "Delete job", position = 4)
    public Response delete(@ApiParam(value = "jobId", required = true) @PathParam("jobId") long jobId,
                           @ApiParam(value = "deleteFiles", required = true) @DefaultValue("true") @QueryParam("deleteFiles") boolean deleteFiles) {
        try {
            List<QueryResult> results = new LinkedList<>();
            if (deleteFiles) {
                QueryResult<Job> jobQueryResult = catalogManager.getJob(jobId, null, sessionId);
                for (Long fileId : jobQueryResult.getResult().get(0).getOutput()) {
                    QueryResult queryResult = catalogManager.deleteFile(fileId, sessionId);
                    results.add(queryResult);
                }
            }
            results.add(catalogManager.deleteJob(jobId, sessionId));
            return createOkResponse(results);
        } catch (CatalogException | IOException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{jobIds}/share")
    @ApiOperation(value = "Share jobs with other members", position = 5)
    public Response share(@PathParam(value = "jobIds") String jobIds,
                          @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members,
                          @ApiParam(value = "Comma separated list of job permissions", required = false) @DefaultValue("") @QueryParam("permissions") String permissions,
                          @ApiParam(value = "Boolean indicating whether to allow the change of of permissions in case any member already had any", required = true) @DefaultValue("false") @QueryParam("override") boolean override) {
        try {
            return createOkResponse(catalogManager.shareJob(jobIds, members, Arrays.asList(permissions.split(",")), override, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{jobIds}/unshare")
    @ApiOperation(value = "Remove the permissions for the list of members", position = 6)
    public Response unshare(@PathParam(value = "jobIds") String jobIds,
                            @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members,
                            @ApiParam(value = "Comma separated list of job permissions", required = false) @DefaultValue("") @QueryParam("permissions") String permissions) {
        try {
            return createOkResponse(catalogManager.unshareJob(jobIds, members, permissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group jobs by several fields", position = 10)
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("") @QueryParam("by") String by,
                            @ApiParam(value = "id", required = false) @DefaultValue("") @QueryParam("id") String id,
                            @ApiParam(value = "studyId", required = true) @DefaultValue("") @QueryParam("studyId") String studyId,
                            @ApiParam(value = "name", required = false) @DefaultValue("") @QueryParam("name") String name,
                            @ApiParam(value = "path", required = false) @DefaultValue("") @QueryParam("path") String path,
                            @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") File.FileStatus status,
                            @ApiParam(value = "ownerId", required = false) @DefaultValue("") @QueryParam("ownerId") String ownerId,
                            @ApiParam(value = "creationDate", required = false) @DefaultValue("") @QueryParam("creationDate") String creationDate,
                            @ApiParam(value = "modificationDate", required = false) @DefaultValue("") @QueryParam("modificationDate") String modificationDate,
                            @ApiParam(value = "description", required = false) @DefaultValue("") @QueryParam("description") String description,
                            @ApiParam(value = "attributes", required = false) @DefaultValue("") @QueryParam("attributes") String attributes) {
        try {
            Query query = new Query();
            QueryOptions qOptions = new QueryOptions();
            parseQueryParams(params, CatalogJobDBAdaptor.QueryParams::getParam, query, qOptions);

            logger.debug("query = " + query.toJson());
            logger.debug("queryOptions = " + qOptions.toJson());
            QueryResult result = catalogManager.jobGroupBy(query, qOptions, by, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}