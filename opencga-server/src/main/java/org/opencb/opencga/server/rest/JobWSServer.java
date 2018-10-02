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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.JobManager;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.acls.AclParams;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;

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

        public InputJob(String id, String name, String toolName, String description, long startTime, long endTime, String commandLine,
                        Status status, String outDirId, List<String> input, Map<String, Object> attributes, Map<String, Object>
                                resourceManagerAttributes) {
            this.id = id;
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

        enum Status {READY, ERROR}

        @ApiModelProperty(required = true)
        public String id;
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
        public List<String> input;
        public List<String> output;
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
    public Response createJobPOST(
            @ApiParam(value = "DEPRECATED: studyId", hidden = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "job", required = true) InputJob inputJob) {
        try {
            inputJob = ObjectUtils.defaultIfNull(inputJob, new InputJob());

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            if (StringUtils.isNotEmpty(inputJob.outDirId) && StringUtils.isEmpty(inputJob.outDir)) {
                inputJob.outDir = inputJob.outDirId;
            }

            Job.JobStatus jobStatus;
            if (Job.JobStatus.isValid(inputJob.status.toString())) {
                jobStatus = new Job.JobStatus(inputJob.status.toString(), inputJob.statusMessage);
            } else {
                jobStatus = new Job.JobStatus();
                jobStatus.setMessage(inputJob.statusMessage);
            }

            String jobId = StringUtils.isEmpty(inputJob.id) ? inputJob.name : inputJob.id;
            String jobName = StringUtils.isEmpty(inputJob.name) ? jobId : inputJob.name;
            Job job = new Job(-1, jobId, jobName, "", inputJob.toolName, null, "", inputJob.description, inputJob.startTime,
                    inputJob.endTime, inputJob.execution, "", inputJob.commandLine, false, jobStatus, -1,
                    new File().setPath(inputJob.outDir), parseToListOfFiles(inputJob.input),
                    parseToListOfFiles(inputJob.output), Collections.emptyList(), inputJob.params, -1, inputJob.attributes,
                    inputJob.resourceManagerAttributes);

            QueryResult<Job> result = catalogManager.getJobManager().create(studyStr, job, queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private List<File> parseToListOfFiles(List<String> longList) {
        if (longList == null || longList.isEmpty()) {
            return Collections.emptyList();
        }
        List<File> fileList = new ArrayList<>(longList.size());
        for (String myLong : longList) {
            fileList.add(new File().setPath(myLong));
        }
        return fileList;
    }

    @GET
    @Path("/{jobIds}/info")
    @ApiOperation(value = "Get job information", position = 2, response = Job[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response info(@ApiParam(value = "Comma separated list of job ids or names up to a maximum of 100", required = true)
                         @PathParam("jobIds") String jobIds,
                         @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
                                 + "exception whenever one of the entries looked for cannot be shown for whichever reason",
                                 defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            List<String> idList = getIdList(jobIds);
            return createOkResponse(catalogManager.getJobManager().get(idList, queryOptions, silent, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Job search method", position = 12, response = Job[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = "DEPRECATED: studyId", hidden = true) @QueryParam("studyId") String studyId,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "name", required = false) @DefaultValue("") @QueryParam("name") String name,
            @ApiParam(value = "tool name", required = false) @DefaultValue("") @QueryParam("toolName") String tool,
            @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") String status,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)")
                @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)")
                @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "ownerId", required = false) @DefaultValue("") @QueryParam("ownerId") String ownerId,
            @ApiParam(value = "date", required = false) @DefaultValue("") @QueryParam("date") String date,
            @ApiParam(value = "Comma separated list of input file ids", required = false) @DefaultValue("") @QueryParam("inputFiles") String inputFiles,
            @ApiParam(value = "Comma separated list of output file ids", required = false) @DefaultValue("")
            @QueryParam("outputFiles") String outputFiles,
            @ApiParam(value = "Release value") @QueryParam("release") String release,
            @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount) {
        try {
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);
            query.remove("study");

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
            QueryResult<Job> result;
            if (count) {
                result = catalogManager.getJobManager().count(studyStr, query, sessionId);
            } else {
                result = catalogManager.getJobManager().get(studyStr, query, queryOptions, sessionId);
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
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "jobId", required = true) @PathParam("jobId") String jobId) {
        try {
            return createOkResponse(catalogManager.getJobManager().visit(studyStr, jobId, sessionId));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }


    @DELETE
    @Path("/delete")
    @ApiOperation(value = "Delete existing jobs")
    public Response delete(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
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
            query.remove("study");
            return createOkResponse(jobManager.delete(studyStr, query, queryOptions, sessionId));
        } catch (Exception e) {

            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group jobs by several fields", position = 10, hidden = true,
            notes = "Only group by categorical variables. Grouping by continuous variables might cause unexpected behaviour")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "count", value = "Count the number of elements matching the group", dataType = "boolean",
                    paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Maximum number of documents (groups) to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "50")
    })
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
                            @QueryParam("fields") String fields,
                            @ApiParam(value = "DEPRECATED: studyId", hidden = true) @DefaultValue("") @QueryParam("studyId") String studyId,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr,
                            @ApiParam(value = "name", required = false) @DefaultValue("") @QueryParam("name") String name,
                            @ApiParam(value = "path", required = false) @DefaultValue("") @QueryParam("path") String path,
                            @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") File.FileStatus status,
                            @ApiParam(value = "ownerId", required = false) @DefaultValue("") @QueryParam("ownerId") String ownerId,
                            @ApiParam(value = "creationDate", required = false) @DefaultValue("")
                            @QueryParam("creationDate") String creationDate) {
        try {
            query.remove("study");
            query.remove("fields");

            if (StringUtils.isEmpty(fields)) {
                throw new CatalogException("Empty fields parameter.");
            }
            QueryResult result = catalogManager.getJobManager().groupBy(studyStr, query, Arrays.asList(fields.split(",")), queryOptions,
                    sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{jobIds}/acl")
    @ApiOperation(value = "Return the acl of the job. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of job ids up to a maximum of 100", required = true) @PathParam("jobIds") String jobIdsStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
                                    + "exception whenever one of the entries looked for cannot be shown for whichever reason",
                                    defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            List<String> idList = getIdList(jobIdsStr);
            return createOkResponse(jobManager.getAcls(null, idList, member, silent, sessionId));
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
            return createOkResponse(jobManager.updateAcl(null, idList, memberId, aclParams, sessionId));
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
            List<String> idList = getIdList(params.job);
            return createOkResponse(jobManager.updateAcl(null, idList, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}