/*
 * Copyright 2015-2017 OpenCB
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

import io.swagger.annotations.*;
import org.apache.commons.lang3.ObjectUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.managers.JobManager;
import org.opencb.opencga.catalog.models.update.JobUpdateParams;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.ToolInfo;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.result.ExecutionResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/{apiVersion}/jobs")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Jobs", description = "Methods for working with 'jobs' endpoint")
public class JobWSServer extends OpenCGAWSServer {

    private JobManager jobManager;

    public JobWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        jobManager = catalogManager.getJobManager();
    }


    public static class InputJob {

        public InputJob() {
        }

        private String id;
        private String name;
        private String description;

        private ToolInfo tool;

        private Enums.Priority priority;

        private String commandLine;

        private Map<String, Object> params;

        private String creationDate;
        private Enums.ExecutionStatus status;

        private TinyFile outDir;
        private List<TinyFile> input;    // input files to this job
        private List<TinyFile> output;   // output files of this job
        private List<String> tags;

        private ExecutionResult result;

        private TinyFile log;
        private TinyFile errorLog;

        private Map<String, Object> attributes;

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }

        public ToolInfo getTool() {
            return tool;
        }

        public Enums.Priority getPriority() {
            return priority;
        }

        public String getCommandLine() {
            return commandLine;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public String getCreationDate() {
            return creationDate;
        }

        public Enums.ExecutionStatus getStatus() {
            return status;
        }

        public TinyFile getOutDir() {
            return outDir;
        }

        public List<TinyFile> getInput() {
            return input != null ? input : Collections.emptyList();
        }

        public List<TinyFile> getOutput() {
            return output != null ? output : Collections.emptyList();
        }

        public List<String> getTags() {
            return tags;
        }

        public ExecutionResult getResult() {
            return result;
        }

        public TinyFile getLog() {
            return log;
        }

        public TinyFile getErrorLog() {
            return errorLog;
        }

        public Map<String, Object> getAttributes() {
            return attributes;
        }

        public Job toJob() {
            return new Job(id, null, name, description, tool, null, commandLine, params, creationDate, null, priority, status,
                    outDir != null ? outDir.toFile() : null,
                    getInput().stream().map(TinyFile::toFile).collect(Collectors.toList()),
                    getOutput().stream().map(TinyFile::toFile).collect(Collectors.toList()),
                    tags, result, false, log != null ? log.toFile() : null, errorLog != null ? errorLog.toFile() : null, 1, null, attributes);
        }

    }

    public class TinyFile {
        private String path;

        public TinyFile() {
        }

        public String getPath() {
            return path;
        }

        public File toFile() {
            return new File().setPath(path);
        }

    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Register an executed job with POST method",
            notes = "Registers a job that has been previously run outside catalog into catalog. <br>"
                    + "Required values: [name, toolName, commandLine, outDirId]", response = Job.class)
    public Response createJobPOST(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "job", required = true) InputJob inputJob) {
        try {
            OpenCGAResult<Job> result = catalogManager.getJobManager().create(studyStr, inputJob.toJob(), queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{jobs}/info")
    @ApiOperation(value = "Get job information", response = Job.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response info(
            @ApiParam(value = ParamConstants.JOBS_DESCRIPTION, required = true) @PathParam("jobs") String jobIds,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Boolean to retrieve deleted jobs", defaultValue = "false") @QueryParam("deleted") boolean deleted) {
        try {
            List<String> idList = getIdList(jobIds);
            return createOkResponse(catalogManager.getJobManager().get(studyStr, idList, new Query("deleted", deleted), queryOptions,
                    true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Job search method", response = Job.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.JOB_TOOL_DESCRIPTION) @QueryParam(ParamConstants.JOB_TOOL_PARAM) String tool,
            @ApiParam(value = ParamConstants.JOB_USER_DESCRIPTION) @QueryParam(ParamConstants.JOB_USER_PARAM) String user,
            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.JOB_PRIORITY_PARAM) String priority,
            @ApiParam(value = ParamConstants.JOB_STATUS_DESCRIPTION) @QueryParam(ParamConstants.JOB_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.JOB_VISITED_DESCRIPTION) @DefaultValue("") @QueryParam(ParamConstants.JOB_VISITED_PARAM) Boolean visited,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.JOB_INPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_INPUT_FILES_PARAM) String input,
            @ApiParam(value = ParamConstants.JOB_OUTPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_OUTPUT_FILES_PARAM) String output,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = "Boolean to retrieve deleted jobs", defaultValue = "false") @QueryParam("deleted") boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(catalogManager.getJobManager().search(studyStr, query, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some job attributes", hidden = true, response = Job.class)
    public Response updateByPost(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.JOB_TOOL_DESCRIPTION) @QueryParam(ParamConstants.JOB_TOOL_PARAM) String tool,
            @ApiParam(value = ParamConstants.JOB_USER_DESCRIPTION) @QueryParam(ParamConstants.JOB_USER_PARAM) String user,
            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.JOB_PRIORITY_PARAM) String priority,
            @ApiParam(value = ParamConstants.JOB_STATUS_DESCRIPTION) @QueryParam(ParamConstants.JOB_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.JOB_VISITED_DESCRIPTION) @DefaultValue("") @QueryParam(ParamConstants.JOB_VISITED_PARAM) Boolean visited,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.JOB_INPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_INPUT_FILES_PARAM) String input,
            @ApiParam(value = ParamConstants.JOB_OUTPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_OUTPUT_FILES_PARAM) String output,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = "Boolean to retrieve deleted jobs", defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = "params") JobUpdateParams parameters) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(jobManager.update(studyStr, query, parameters, true, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{jobs}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some job attributes", response = Job.class)
    public Response updateByPost(
            @ApiParam(value = ParamConstants.JOBS_DESCRIPTION, required = true) @PathParam("jobs") String jobStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "params") JobUpdateParams parameters) {
        try {
            return createOkResponse(jobManager.update(studyStr, getIdList(jobStr), parameters, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/{jobs}/delete")
    @ApiOperation(value = "Delete existing jobs", response = Job.class)
    public Response delete(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of job ids") @PathParam("jobs") String jobs) {
        try {
            return createOkResponse(jobManager.delete(studyStr, getIdList(jobs), queryOptions, true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{jobs}/acl")
    @ApiOperation(value = "Return the acl of the job. If member is provided, it will only return the acl for the member.", response = Map.class)
    public Response getAcls(@ApiParam(value = ParamConstants.JOBS_DESCRIPTION, required = true) @PathParam("jobs") String jobIdsStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION, defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        try {
            List<String> idList = getIdList(jobIdsStr);
            return createOkResponse(jobManager.getAcls(null, idList, member, silent, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class JobAcl extends AclParams {
        public String job;
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", response = Map.class)
    public Response updateAcl(
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = "JSON containing the parameters to add ACLs", required = true) JobAcl params) {
        try {
            ObjectUtils.defaultIfNull(params, new JobAcl());
            AclParams aclParams = new AclParams(params.getPermissions(), params.getAction());
            List<String> idList = getIdList(params.job, false);
            return createOkResponse(jobManager.updateAcl(null, idList, memberId, aclParams, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}