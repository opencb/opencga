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
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.JobManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.analysis.result.Execution;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.common.Enums;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/{apiVersion}/jobs")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Jobs", position = 5, description = "Methods for working with 'jobs' endpoint")
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

        public InputJob(String id, String name, String description, String commandLine, Map<String, Object> params, Enums.ExecutionStatus status) {
            this.id = id;
            this.name = name;
            this.description = description;
            this.commandLine = commandLine;
            this.params = params;
            this.status = status;
        }

        private String id;
        private String name;
        private String description;

        private String toolId;

        private Enums.Priority priority;

        private String commandLine;

        private Map<String, Object> params;

        private String creationDate;
        private Enums.ExecutionStatus status;

        private TinyFile outDir;
        private List<TinyFile> input;    // input files to this job
        private List<TinyFile> output;   // output files of this job
        private List<String> tags;

        private Execution result;

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

        public String getToolId() {
            return toolId;
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

        public Execution getResult() {
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
            return new Job(id, null, name, description, toolId, null, commandLine, params, creationDate, null, priority, status,
                    outDir != null ? outDir.toFile() : null,
                    getInput().stream().map(TinyFile::toFile).collect(Collectors.toList()),
                    getOutput().stream().map(TinyFile::toFile).collect(Collectors.toList()),
                    tags, result, log != null ? log.toFile() : null, errorLog != null ? errorLog.toFile() : null, 1, attributes);
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

    // TODO: Change the name for register. We are not "creating" a job, meaning that it will be put into execution, we are just registering
    // TODO: it, so it would be necessary changing the path name "create" per "register"
    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Register an executed job with POST method",
            notes = "Registers a job that has been previously run outside catalog into catalog. <br>"
                    + "Required values: [name, toolName, commandLine, outDirId]", response = Job.class)
    public Response createJobPOST(
            @ApiParam(value = "DEPRECATED: studyId", hidden = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "job", required = true) InputJob inputJob) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }

            DataResult<Job> result = catalogManager.getJobManager().create(studyStr, inputJob.toJob(), queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/execute")
    @ApiOperation(value = "Execute an analysis using an internal or external tool", response = Job.class)
    public Response execute(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM)String studyStr,
            @ApiParam(value = "Analysis id") @QueryParam("analysisId")String analysisId,
            @ApiParam(value = "Json containing the execution parameters", required = true) JobExecutionParams params) {
        try {
            DataResult<Job> queryResult = catalogManager.getJobManager().submit(studyStr, analysisId,  Enums.Priority.MEDIUM, params.params,
                    token);
            return createOkResponse(queryResult);
        } catch(Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{jobIds}/info")
    @ApiOperation(value = "Get job information", position = 2, response = Job[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response info(
            @ApiParam(value = ParamConstants.JOBS_DESCRIPTION, required = true) @PathParam("jobIds") String jobIds,
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
    @ApiOperation(value = "Job search method", position = 12, response = Job[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = "DEPRECATED: studyId", hidden = true) @QueryParam("studyId") String studyId,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "name") @DefaultValue("") @QueryParam("name") String name,
            @ApiParam(value = "tool name") @DefaultValue("") @QueryParam("toolName") String tool,
            @ApiParam(value = "status") @DefaultValue("") @QueryParam("status") String status,
            @ApiParam(value = "Boolean to retrieve deleted jobs", defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam("creationDate") String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "ownerId") @DefaultValue("") @QueryParam("ownerId") String ownerId,
            @ApiParam(value = "date") @DefaultValue("") @QueryParam("date") String date,
            @ApiParam(value = "Comma separated list of input file ids") @DefaultValue("") @QueryParam("inputFiles") String inputFiles,
            @ApiParam(value = "Comma separated list of output file ids") @DefaultValue("")
            @QueryParam("outputFiles") String outputFiles,
            @ApiParam(value = "Release value") @QueryParam("release") String release,
            @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount) {
        try {
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);
            query.remove(ParamConstants.STUDY_PARAM);

            if (StringUtils.isNotEmpty(studyId)) {
                studyStr = studyId;
            }

            // TODO this must be changed: only one queryOptions need to be passed
            if (query.containsKey(JobDBAdaptor.QueryParams.NAME.key())
                    && (query.get(JobDBAdaptor.QueryParams.NAME.key()) == null
                    || query.getString(JobDBAdaptor.QueryParams.NAME.key()).isEmpty())) {
                query.remove(JobDBAdaptor.QueryParams.NAME.key());
                logger.debug("Name attribute empty, it's been removed");
            }
            DataResult<Job> result;
            if (count) {
                result = catalogManager.getJobManager().count(studyStr, query, token);
            } else {
                result = catalogManager.getJobManager().search(studyStr, query, queryOptions, token);
            }
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{jobId}/visit")
    @ApiOperation(value = "Increment job visits", position = 3)
    public Response visit(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "jobId", required = true) @PathParam("jobId") String jobId) {
        try {
            return createOkResponse(catalogManager.getJobManager().visit(studyStr, jobId, token));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/{jobs}/delete")
    @ApiOperation(value = "Delete existing jobs")
    public Response delete(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of job ids") @PathParam("jobs") String jobs) {
        try {
            return createOkResponse(jobManager.delete(studyStr, getIdList(jobs), queryOptions, true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/delete")
    @ApiOperation(value = "Delete existing jobs")
    public Response delete(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "id") @DefaultValue("") @QueryParam("id") String id,
            @ApiParam(value = "name") @QueryParam("name") String name,
            @ApiParam(value = "tool name") @QueryParam("toolName") String tool,
            @ApiParam(value = "status") @QueryParam("status") String status,
            @ApiParam(value = "ownerId") @QueryParam("ownerId") String ownerId,
            @ApiParam(value = "date") @QueryParam("date") String date,
            @ApiParam(value = "Comma separated list of input file ids") @QueryParam("inputFiles") String inputFiles,
            @ApiParam(value = "Comma separated list of output file ids") @QueryParam("outputFiles") String outputFiles,
            @ApiParam(value = "Release value") @QueryParam("release") String release) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(jobManager.delete(studyStr, query, queryOptions, true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group jobs by several fields", position = 10, hidden = true,
            notes = "Only group by categorical variables. Grouping by continuous variables might cause unexpected behaviour")
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.COUNT, value = "Count the number of elements matching the group", dataType = "boolean",
                    paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Maximum number of documents (groups) to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "50")
    })
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
                            @QueryParam("fields") String fields,
                            @ApiParam(value = "DEPRECATED: studyId", hidden = true) @DefaultValue("") @QueryParam("studyId") String studyId,
                            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                            @ApiParam(value = "name") @DefaultValue("") @QueryParam("name") String name,
                            @ApiParam(value = "path") @DefaultValue("") @QueryParam("path") String path,
                            @ApiParam(value = "status") @DefaultValue("") @QueryParam("status") File.FileStatus status,
                            @ApiParam(value = "ownerId") @DefaultValue("") @QueryParam("ownerId") String ownerId,
                            @ApiParam(value = "creationDate") @DefaultValue("")
                            @QueryParam("creationDate") String creationDate) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("fields");

            if (StringUtils.isEmpty(fields)) {
                throw new CatalogException("Empty fields parameter.");
            }
            DataResult result = catalogManager.getJobManager().groupBy(studyStr, query, Arrays.asList(fields.split(",")), queryOptions,
                    token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{jobIds}/acl")
    @ApiOperation(value = "Return the acl of the job. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(@ApiParam(value = ParamConstants.JOBS_DESCRIPTION, required = true) @PathParam("jobIds") String jobIdsStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION, defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        try {
            List<String> idList = getIdList(jobIdsStr);
            return createOkResponse(jobManager.getAcls(null, idList, member, silent, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{jobId}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member [DEPRECATED]", position = 21, hidden = true,
            notes = "DEPRECATED: The usage of this webservice is discouraged. A different entrypoint /acl/{members}/update has been added "
                    + "to also support changing permissions using queries.")
    public Response updateAclPOST(
            @ApiParam(value = "jobId", required = true) @PathParam("jobId") String jobIdStr,
            @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value = "JSON containing one of the keys 'add', 'set' or 'remove'", required = true) StudyWSServer.MemberAclUpdateOld params) {
        try {
            AclParams aclParams = getAclParams(params.add, params.remove, params.set);
            List<String> idList = getIdList(jobIdStr);
            return createOkResponse(jobManager.updateAcl(null, idList, memberId, aclParams, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class JobAcl extends AclParams {
        public String job;
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
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

    public class JobExecutionParams {
        private String id;
        private String name;
        private String description;

        private Map<String, Object> params;

        public JobExecutionParams() {
        }

        public String getId() {
            return id;
        }

        public JobExecutionParams setId(String id) {
            this.id = id;
            return this;
        }

        public String getName() {
            return name;
        }

        public JobExecutionParams setName(String name) {
            this.name = name;
            return this;
        }

        public String getDescription() {
            return description;
        }

        public JobExecutionParams setDescription(String description) {
            this.description = description;
            return this;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public JobExecutionParams setParams(Map<String, Object> params) {
            this.params = params;
            return this;
        }
    }

}