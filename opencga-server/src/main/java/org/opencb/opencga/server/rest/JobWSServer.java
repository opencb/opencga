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
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.api.IJobManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Path("/{version}/jobs")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Jobs", position = 5, description = "Methods for working with 'jobs' endpoint")
public class JobWSServer extends OpenCGAWSServer {

    private IJobManager jobManager;

    public JobWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        jobManager = catalogManager.getJobManager();
    }


    public static class InputJob {
        public InputJob() {
        }

        public InputJob(String name, String toolName, String description, long startTime, long endTime, String commandLine, Status status,
                        String outDirId, List<Long> input, Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes) {
            this.name = name;
            this.toolName = toolName;
            this.description = description;
            this.startTime = startTime;
            this.endTime = endTime;
            this.commandLine = commandLine;
            this.status = status;
            this.outDir = outDirId;
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
        public String outDirId;
        @ApiModelProperty(required = true)
        public String outDir;
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
                    + "Required values: [name, toolName, commandLine, outDirId]", response = Job.class)
    public Response createJobPOST(@ApiParam(value = "DEPRECATED: studyId", hidden = true) @QueryParam("studyId") String studyIdStr,
                                  @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                  @QueryParam("study") String studyStr,
                                  @ApiParam(value = "job", required = true) InputJob job) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            if (StringUtils.isNotEmpty(job.outDirId) && StringUtils.isEmpty(job.outDir)) {
                job.outDir = job.outDirId;
            }
            long studyId = catalogManager.getStudyId(studyStr, sessionId);
            Job.JobStatus jobStatus;
            if (Job.JobStatus.isValid(job.status.toString())) {
                jobStatus = new Job.JobStatus(job.status.toString(), job.statusMessage);
            } else {
                jobStatus = new Job.JobStatus();
                jobStatus.setMessage(job.statusMessage);
            }
            long outDir = catalogManager.getFileId(job.outDir, Long.toString(studyId), sessionId);
            QueryResult<Job> result = catalogManager.createJob(studyId, job.name, job.toolName, job.description, job.execution, job.params,
                    job.commandLine, null, outDir, parseToListOfFiles(job.input), parseToListOfFiles(job.output), job.attributes,
                    job.resourceManagerAttributes, jobStatus, job.startTime, job.endTime, queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private List<File> parseToListOfFiles(List<Long> longList) {
        if (longList == null || longList.size() == 0) {
            return Collections.emptyList();
        }
        List<File> fileList = new ArrayList<>(longList.size());
        for (Long myLong : longList) {
            fileList.add(new File().setId(myLong));
        }
        return fileList;
    }

    @GET
    @Path("/{jobId}/info")
    @ApiOperation(value = "Get job information", position = 2, response = Job[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response info(@ApiParam(value = "jobId", required = true) @PathParam("jobId") long jobId) {
        try {
            return createOkResponse(catalogManager.getJob(jobId, queryOptions, sessionId));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    // FIXME: Implement and change parameters
    @GET
    @Path("/search")
    @ApiOperation(value = "Filter jobs [PENDING]", position = 12, response = Job[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response search(@ApiParam(value = "DEPRECATED: studyId", hidden = true) @QueryParam("studyId") String studyId,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                               @QueryParam("study") String studyStr,
                           @ApiParam(value = "name", required = false) @DefaultValue("") @QueryParam("name") String name,
                           @ApiParam(value = "tool name", required = false) @DefaultValue("") @QueryParam("toolName") String tool,
                           @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") String status,
                           @ApiParam(value = "ownerId", required = false) @DefaultValue("") @QueryParam("ownerId") String ownerId,
                           @ApiParam(value = "date", required = false) @DefaultValue("") @QueryParam("date") String date,
                           @ApiParam(value = "Comma separated list of input file ids", required = false) @DefaultValue("") @QueryParam("inputFiles") String inputFiles,
                           @ApiParam(value = "Comma separated list of output file ids", required = false) @DefaultValue("")
                               @QueryParam ("outputFiles") String outputFiles,
                           @ApiParam(value = "Release value") @QueryParam("release") String release,
                           @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount) {
        try {
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            if (StringUtils.isNotEmpty(studyId)) {
                studyStr = studyId;
            }
            long studyIdNum = catalogManager.getStudyId(studyStr, sessionId);
            // TODO this must be changed: only one queryOptions need to be passed
            if (query.containsKey(JobDBAdaptor.QueryParams.NAME.key())
                    && (query.get(JobDBAdaptor.QueryParams.NAME.key()) == null
                    || query.getString(JobDBAdaptor.QueryParams.NAME.key()).isEmpty())) {
                query.remove(JobDBAdaptor.QueryParams.NAME.key());
                logger.debug("Name attribute empty, it's been removed");
            }
            QueryResult<Job> result;
            if (count) {
                result = catalogManager.getJobManager().count(Long.toString(studyIdNum), query, sessionId);
            } else {
                result = catalogManager.getAllJobs(studyIdNum, query, queryOptions, sessionId);
            }
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
    @Path("/{jobIds}/delete")
    @ApiOperation(value = "Delete job [NOT TESTED]", position = 4)
    public Response delete(@ApiParam(value = "Comma separated list of job ids or names", required = true) @PathParam("jobIds")
                                       String jobIds,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                               @QueryParam("study") String studyStr) {
//                           @ApiParam(value = "deleteFiles", required = false) @DefaultValue("true")
//                                @QueryParam("deleteFiles") boolean deleteFiles) {
        try {
//            QueryOptions options = new QueryOptions(JobManager.DELETE_FILES, deleteFiles);
            List<QueryResult<Job>> delete = catalogManager.getJobManager().delete(jobIds, studyStr, queryOptions, sessionId);
            return createOkResponse(delete);
        } catch (CatalogException | IOException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group jobs by several fields", position = 10)
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
                                @QueryParam("fields") String fields,
                            @ApiParam(value = "id", required = false) @DefaultValue("") @QueryParam("id") String id,
                            @ApiParam(value = "DEPRECATED: studyId", hidden = true) @DefaultValue("") @QueryParam("studyId") String studyId,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr,
                            @ApiParam(value = "name", required = false) @DefaultValue("") @QueryParam("name") String name,
                            @ApiParam(value = "path", required = false) @DefaultValue("") @QueryParam("path") String path,
                            @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") File.FileStatus status,
                            @ApiParam(value = "ownerId", required = false) @DefaultValue("") @QueryParam("ownerId") String ownerId,
                            @ApiParam(value = "creationDate", required = false) @DefaultValue("")
                                @QueryParam("creationDate") String creationDate,
//                            @ApiParam(value = "modificationDate", required = false) @DefaultValue("")
//                              @QueryParam("modificationDate") String modificationDate,
                            @ApiParam(value = "description", required = false) @DefaultValue("")
                                @QueryParam("description") String description,
                            @ApiParam(value = "attributes", required = false) @DefaultValue("")
                                @QueryParam("attributes") String attributes) {
        try {
            QueryResult result = catalogManager.jobGroupBy(studyStr, query, queryOptions, fields, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{jobIds}/acl")
    @ApiOperation(value = "Return the acl of the job. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of job ids", required = true) @PathParam("jobIds") String jobIdsStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member) {
        try {
            if (StringUtils.isEmpty(member)) {
                return createOkResponse(catalogManager.getAllJobAcls(jobIdsStr, sessionId));
            } else {
                return createOkResponse(catalogManager.getJobAcl(jobIdsStr, member, sessionId));
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{jobId}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member [DEPRECATED]", position = 21,
            notes = "DEPRECATED: The usage of this webservice is discouraged. A different entrypoint /acl/{members}/update has been added "
                    + "to also support changing permissions using queries.")
    public Response updateAclPOST(
            @ApiParam(value = "jobId", required = true) @PathParam("jobId") String jobIdStr,
            @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value="JSON containing one of the keys 'add', 'set' or 'remove'", required = true) StudyWSServer.MemberAclUpdateOld params) {
        try {
            AclParams aclParams = getAclParams(params.add, params.remove, params.set);
            return createOkResponse(jobManager.updateAcl(jobIdStr, null, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class JobAcl extends AclParams {
        public String job;
    }

    @POST
    @Path("/acl/{memberIds}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("memberIds") String memberId,
            @ApiParam(value="JSON containing the parameters to add ACLs", required = true) JobAcl params) {
        try {
            AclParams aclParams = new AclParams(params.getPermissions(), params.getAction());
            return createOkResponse(jobManager.updateAcl(params.job, null, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}