/*
 * Copyright 2015-2020 OpenCB
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

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.managers.JobManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.file.FileContent;
import org.opencb.opencga.core.models.job.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.JOB_DEPENDS_ON;

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

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Register an executed job with POST method",
            notes = "Registers a job that has been previously run outside catalog into catalog. <br>"
                    + "Required values: [name, toolName, commandLine, outDirId]", response = Job.class)
    public Response createJobPOST(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "job", required = true) JobCreateParams inputJob) {
        try {
            OpenCGAResult<Job> result = catalogManager.getJobManager().create(studyStr, inputJob.toJob(), queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/retry")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Relaunch a failed job", response = Job.class)
    public Response retryJob(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobId,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTagsStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = "job", required = true) JobRetryParams params

    ) {
        try {
            List<String> jobDependsOn;
            if (StringUtils.isNotEmpty(dependsOn)) {
                jobDependsOn = Arrays.asList(dependsOn.split(","));
            } else {
                jobDependsOn = Collections.emptyList();
            }

            List<String> jobTags;
            if (StringUtils.isNotEmpty(jobTagsStr)) {
                jobTags = Arrays.asList(jobTagsStr.split(","));
            } else {
                jobTags = Collections.emptyList();
            }
            OpenCGAResult<Job> result = catalogManager.getJobManager().retry(study, params,
                    null, jobId, jobDescription, jobDependsOn, jobTags, token);
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
    @Path("/{job}/log/head")
    @ApiOperation(value = "Show the first lines of a log file (up to a limit)", response = FileContent.class)
    public Response log(
            @ApiParam(value = ParamConstants.JOB_ID_DESCRIPTION, required = true) @PathParam("job") String jobId,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Starting byte from which the file will be read") @QueryParam("offset") long offset,
            @ApiParam(value = ParamConstants.MAXIMUM_LINES_CONTENT_DESCRIPTION, defaultValue = "20") @QueryParam("lines") Integer lines,
            @ApiParam(value = "Log file to be shown (stdout or stderr)") @DefaultValue("stderr") @QueryParam("type") String type) {
        try {
            if (lines == null) {
                lines = 20;
            }
            ParamUtils.checkIsSingleID(jobId);
            return createOkResponse(catalogManager.getJobManager().log(studyStr, jobId, offset, lines, type, false, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{job}/log/tail")
    @ApiOperation(value = "Show the last lines of a log file (up to a limit)", response = FileContent.class)
    public Response log(
            @ApiParam(value = ParamConstants.JOB_ID_DESCRIPTION, required = true) @PathParam("job") String jobId,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.MAXIMUM_LINES_CONTENT_DESCRIPTION, defaultValue = "20") @QueryParam("lines") Integer lines,
            @ApiParam(value = "Log file to be shown (stdout or stderr)") @DefaultValue("stderr") @QueryParam("type") String type) {
        try {
            if (lines == null) {
                lines = 20;
            }
            ParamUtils.checkIsSingleID(jobId);
            return createOkResponse(catalogManager.getJobManager().log(studyStr, jobId, 0, lines, type, true, token));
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
            @ApiParam(value = ParamConstants.OTHER_STUDIES_FLAG_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.OTHER_STUDIES_FLAG) boolean others,
            @ApiParam(value = ParamConstants.JOB_IDS_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.JOB_UUIDS_DESCRIPTION) @QueryParam(ParamConstants.JOB_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.JOB_TOOL_ID_DESCRIPTION) @QueryParam(ParamConstants.JOB_TOOL_ID_PARAM) String toolId,
            @ApiParam(value = ParamConstants.JOB_TOOL_TYPE_DESCRIPTION) @QueryParam(ParamConstants.JOB_TOOL_TYPE_PARAM) String toolType,
            @ApiParam(value = ParamConstants.JOB_USER_DESCRIPTION) @QueryParam(ParamConstants.JOB_USER_PARAM) String user,
            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.JOB_PRIORITY_PARAM) String priority,
            @ApiParam(value = ParamConstants.JOB_STATUS_DESCRIPTION) @QueryParam(ParamConstants.JOB_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.JOB_INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.JOB_INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.JOB_VISITED_DESCRIPTION) @DefaultValue("") @QueryParam(ParamConstants.JOB_VISITED_PARAM) Boolean visited,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.JOB_INPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_INPUT_FILES_PARAM) String input,
            @ApiParam(value = ParamConstants.JOB_OUTPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_OUTPUT_FILES_PARAM) String output,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam("deleted") boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(catalogManager.getJobManager().search(studyStr, query, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/distinct")
    @ApiOperation(value = "Job distinct method", response = Object.class)
    public Response distinct(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.OTHER_STUDIES_FLAG_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.OTHER_STUDIES_FLAG) boolean others,
            @ApiParam(value = ParamConstants.JOB_IDS_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.JOB_UUIDS_DESCRIPTION) @QueryParam(ParamConstants.JOB_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.JOB_TOOL_ID_DESCRIPTION) @QueryParam(ParamConstants.JOB_TOOL_ID_PARAM) String toolId,
            @ApiParam(value = ParamConstants.JOB_TOOL_TYPE_DESCRIPTION) @QueryParam(ParamConstants.JOB_TOOL_TYPE_PARAM) String toolType,
            @ApiParam(value = ParamConstants.JOB_USER_DESCRIPTION) @QueryParam(ParamConstants.JOB_USER_PARAM) String user,
            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.JOB_PRIORITY_PARAM) String priority,
            @ApiParam(value = ParamConstants.JOB_STATUS_DESCRIPTION) @QueryParam(ParamConstants.JOB_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.JOB_INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.JOB_INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.JOB_VISITED_DESCRIPTION) @DefaultValue("") @QueryParam(ParamConstants.JOB_VISITED_PARAM) Boolean visited,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.JOB_INPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_INPUT_FILES_PARAM) String input,
            @ApiParam(value = ParamConstants.JOB_OUTPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_OUTPUT_FILES_PARAM) String output,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);
            return createOkResponse(jobManager.distinct(studyStr, field, query, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @POST
//    @Path("/update")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "Update some job attributes", hidden = true, response = Job.class)
//    public Response updateByPost(
//            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
//            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID_PARAM) String name,
//            @ApiParam(value = ParamConstants.JOB_TOOL_ID_DESCRIPTION) @QueryParam(ParamConstants.JOB_TOOL_ID_PARAM) String tool,
//            @ApiParam(value = ParamConstants.JOB_USER_DESCRIPTION) @QueryParam(ParamConstants.JOB_USER_PARAM) String user,
//            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.JOB_PRIORITY_PARAM) String priority,
//            @ApiParam(value = ParamConstants.JOB_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String status,
//            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
//            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
//            @ApiParam(value = ParamConstants.JOB_VISITED_DESCRIPTION) @DefaultValue("") @QueryParam(ParamConstants.JOB_VISITED_PARAM) Boolean visited,
//            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS_PARAM) String tags,
//            @ApiParam(value = ParamConstants.JOB_INPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_INPUT_FILES_PARAM) String input,
//            @ApiParam(value = ParamConstants.JOB_OUTPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_OUTPUT_FILES_PARAM) String output,
//            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
//            @ApiParam(value = "Boolean to retrieve deleted jobs", defaultValue = "false") @QueryParam("deleted") boolean deleted,
//            @ApiParam(value = "body") JobUpdateParams parameters) {
//        try {
//            query.remove(ParamConstants.STUDY_PARAM);
//            return createOkResponse(jobManager.update(studyStr, query, parameters, true, queryOptions, token));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @POST
    @Path("/{jobs}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some job attributes", response = Job.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response updateByPost(
            @ApiParam(value = ParamConstants.JOBS_DESCRIPTION, required = true) @PathParam("jobs") String jobStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) Boolean includeResult,
            @ApiParam(value = "body") JobUpdateParams parameters) {
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
    @Path("/top")
    @ApiOperation(value = "Provide a summary of the running jobs", response = JobTop.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Maximum number of jobs to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "20")
    })
    public Response top(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.JOB_PRIORITY_PARAM) String priority,
            @ApiParam(value = ParamConstants.JOB_USER_DESCRIPTION) @QueryParam(ParamConstants.JOB_USER_PARAM) String user,
            @ApiParam(value = ParamConstants.JOB_TOOL_ID_DESCRIPTION) @QueryParam(ParamConstants.JOB_TOOL_ID_PARAM) String tool) {
        query.remove(JobDBAdaptor.QueryParams.STUDY.key());
        if (limit == 0) {
            limit = 20;
        }
        return run(() -> catalogManager.getJobManager().top(study, query, limit, token));
    }


    @GET
    @Path("/{jobs}/acl")
    @ApiOperation(value = "Return the acl of the job. If member is provided, it will only return the acl for the member.", response = AclEntryList.class)
    public Response getAcls(@ApiParam(value = ParamConstants.JOBS_DESCRIPTION, required = true) @PathParam("jobs") String jobIdsStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION, defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        return run(() -> {
            List<String> idList = getIdList(jobIdsStr);
            return jobManager.getAcls(null, idList, member, silent, token);
        });
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", response = AclEntryList.class)
    public Response updateAcl(
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = ParamConstants.ACL_ACTION_DESCRIPTION, required = true, defaultValue = "ADD") @QueryParam(ParamConstants.ACL_ACTION_PARAM) ParamUtils.AclAction action,
            @ApiParam(value = "JSON containing the parameters to add ACLs", required = true) JobAclUpdateParams params) {
        try {
            ObjectUtils.defaultIfNull(params, new JobAclUpdateParams());
            AclParams aclParams = new AclParams(params.getPermissions());
            List<String> idList = getIdList(params.getJob(), false);
            return createOkResponse(jobManager.updateAcl(null, idList, memberId, aclParams, action, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Fetch catalog job stats", response = FacetField.class)
    public Response getAggregationStats(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Tool id") @QueryParam("toolId") String toolId,
            @ApiParam(value = "Tool scope") @QueryParam("toolScope") String toolScope,
            @ApiParam(value = "Tool type") @QueryParam("toolType") String toolType,
            @ApiParam(value = "Tool resource") @QueryParam("toolResource") String toolResource,
            @ApiParam(value = "User id") @QueryParam("userId") String userId,
            @ApiParam(value = "Priority") @QueryParam("priority") String priority,
            @ApiParam(value = "Tags") @QueryParam("tags") String tags,
            @ApiParam(value = "Executor id") @QueryParam("executorId") String executorId,
            @ApiParam(value = "Executor framework") @QueryParam("executorFramework") String executorFramework,
            @ApiParam(value = "Creation year") @QueryParam("creationYear") String creationYear,
            @ApiParam(value = "Creation month (JANUARY, FEBRUARY...)") @QueryParam("creationMonth") String creationMonth,
            @ApiParam(value = "Creation day") @QueryParam("creationDay") String creationDay,
            @ApiParam(value = "Creation day of week (MONDAY, TUESDAY...)") @QueryParam("creationDayOfWeek") String creationDayOfWeek,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Release") @QueryParam("release") String release,
            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,
            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            DataResult<FacetField> queryResult = catalogManager.getJobManager().facet(studyStr, query, queryOptions, defaultStats,
                    token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}