package org.opencb.opencga.server.rest;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.managers.ExecutionManager;
import org.opencb.opencga.catalog.managers.JobManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.file.FileContent;
import org.opencb.opencga.core.models.job.*;
import org.opencb.opencga.core.tools.annotations.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.api.ParamConstants.EXECUTION_DEPENDS_ON;

@Path("/{apiVersion}/executions")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Executions", description = "Methods for working with 'executions' endpoint")
public class ExecutionWSServer extends OpenCGAWSServer {

    private final ExecutionManager executionManager;
    private final JobManager jobManager;

    public ExecutionWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        executionManager = catalogManager.getExecutionManager();
        jobManager = catalogManager.getJobManager();
    }

    /*
    1st level: Executions !!
     */
    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Register a past execution",
            notes = "Registers an execution that has been previously run outside catalog into catalog.", response = Execution.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response create(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) Boolean includeResult,
            @ApiParam(value = "execution", required = true) ExecutionCreateParams execution) {
        return run(() -> executionManager.register(studyStr, execution, queryOptions, token));
    }


    @POST
    @Path("/retry")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Relaunch a failed execution", response = Execution.class)
    public Response retry(
            @ApiParam(value = ParamConstants.EXECUTION_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_ID) String executionID,
            @ApiParam(value = ParamConstants.EXECUTION_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.EXECUTION_DEPENDS_ON_DESCRIPTION) @QueryParam(EXECUTION_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.EXECUTION_TAGS_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_TAGS) String jobTagsStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = "job", required = true) ExecutionRetryParams params) {
        return run(() -> {
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
            return executionManager.retry(study, params, null, executionID, jobDescription, jobDependsOn, jobTags, token);
        });
    }


    @POST
    @Path("/{executions}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some execution attributes", response = Execution.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response update(
            @ApiParam(value = ParamConstants.EXECUTIONS_DESCRIPTION, required = true) @PathParam("executions") String executionStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) Boolean includeResult,
            @ApiParam(value = "body") ExecutionUpdateParams parameters) {
        return run(() -> executionManager.update(studyStr, getIdList(executionStr), parameters, queryOptions, token));
    }


    @GET
    @Path("/{executions}/info")
    @ApiOperation(value = "Get execution information", response = Execution.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response info(
            @ApiParam(value = ParamConstants.EXECUTIONS_DESCRIPTION, required = true) @PathParam("executions") String jobIds,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Boolean to retrieve deleted executions", defaultValue = "false") @QueryParam("deleted") boolean deleted) {
        return run(() -> {
            List<String> idList = getIdList(jobIds);
            return executionManager.get(studyStr, idList, new Query("deleted", deleted), queryOptions, true, token);
        });
    }


    @GET
    @Path("/search")
    @ApiOperation(value = "Execution search method", response = Execution.class)
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
            @ApiParam(value = ParamConstants.EXECUTION_IDS_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.EXECUTION_UUIDS_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.EXECUTION_TOOL_ID_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_TOOL_ID_PARAM) String toolId,
            @ApiParam(value = ParamConstants.EXECUTION_USER_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_USER_PARAM) String user,
            @ApiParam(value = ParamConstants.EXECUTION_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_PRIORITY_PARAM) String priority,
            @ApiParam(value = ParamConstants.EXECUTION_INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.EXECUTION_VISITED_DESCRIPTION) @DefaultValue("") @QueryParam(ParamConstants.EXECUTION_VISITED_PARAM) Boolean visited,
            @ApiParam(value = ParamConstants.EXECUTION_IS_PIPELINE_DESCRIPTION) @DefaultValue("") @QueryParam(ParamConstants.EXECUTION_IS_PIPELINE_PARAM) Boolean isPipeline,
            @ApiParam(value = ParamConstants.EXECUTION_TAGS_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam("deleted") boolean deleted) {
        return run(() -> {
            query.remove(ParamConstants.STUDY_PARAM);
            return executionManager.search(studyStr, query, queryOptions, token);
        });
    }


    @GET
    @Path("/distinct")
    @ApiOperation(value = "Execution distinct method", response = Object.class)
    public Response distinct(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.OTHER_STUDIES_FLAG_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.OTHER_STUDIES_FLAG) boolean others,
            @ApiParam(value = ParamConstants.EXECUTION_IDS_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.EXECUTION_UUIDS_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.EXECUTION_TOOL_ID_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_TOOL_ID_PARAM) String toolId,
            @ApiParam(value = ParamConstants.EXECUTION_USER_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_USER_PARAM) String user,
            @ApiParam(value = ParamConstants.EXECUTION_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_PRIORITY_PARAM) String priority,
            @ApiParam(value = ParamConstants.EXECUTION_INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.EXECUTION_VISITED_DESCRIPTION) @DefaultValue("") @QueryParam(ParamConstants.EXECUTION_VISITED_PARAM) Boolean visited,
            @ApiParam(value = ParamConstants.EXECUTION_IS_PIPELINE_DESCRIPTION) @DefaultValue("") @QueryParam(ParamConstants.EXECUTION_IS_PIPELINE_PARAM) Boolean isPipeline,
            @ApiParam(value = ParamConstants.EXECUTION_TAGS_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        return run(() -> {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);
            return executionManager.distinct(studyStr, field, query, token);
        });
    }


    @GET
    @Path("/{executions}/acl")
    @ApiOperation(value = "Return the acl of the execution. If member is provided, it will only return the acl for the member.", response = Map.class)
    public Response getAcls(
            @ApiParam(value = ParamConstants.EXECUTIONS_DESCRIPTION, required = true) @PathParam("executions") String executionIdsStr,
            @ApiParam(value = "User or group id") @QueryParam("member") String member,
            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION, defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        return run(() -> {
            List<String> idList = getIdList(executionIdsStr);
            return executionManager.getAcls(null, idList, member, silent, token);
        });
    }


    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", response = Map.class)
    public Response updateAcl(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = ParamConstants.ACL_ACTION_DESCRIPTION, required = true, defaultValue = "ADD") @QueryParam(ParamConstants.ACL_ACTION_PARAM) ParamUtils.AclAction action,
            @ApiParam(value = "JSON containing the parameters to add ACLs", required = true) ExecutionAclUpdateParams params) {
        return run(() -> {
            AclParams aclParams = new AclParams(params.getPermissions());
            List<String> idList = getIdList(params.getExecution(), false);
            return executionManager.updateAcl(studyStr, idList, memberId, aclParams, action, token);
        });
    }


    @GET
    @Path("/top")
    @ApiOperation(value = "Provide a summary of the running executions", response = JobTop.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Maximum number of executions to be returned", dataType = "integer", paramType = "query", defaultValue = "20")
    })
    public Response top(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.EXECUTION_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_PRIORITY_PARAM) String priority,
            @ApiParam(value = ParamConstants.EXECUTION_USER_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_USER_PARAM) String user,
            @ApiParam(value = ParamConstants.EXECUTION_TOOL_ID_DESCRIPTION) @QueryParam(ParamConstants.EXECUTION_TOOL_ID_PARAM) String tool) {
        return run(() -> {
            query.remove(JobDBAdaptor.QueryParams.STUDY.key());
            if (limit == 0) {
                limit = 20;
            }
            throw new NotImplementedException("Not yet implemented");
        });
    }


    @DELETE
    @Path("/{executions}/delete")
    @ApiOperation(value = "Delete existing executions", response = Execution.class)
    public Response delete(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of execution ids") @PathParam("executions") String executions) {
        return run(() -> {
            throw new NotImplementedException("Not yet implemented");
//            return createOkResponse(executionManager.delete(studyStr, getIdList(executions), queryOptions, true, token));
        });
    }

    /*
    2nd level: Jobs !!
     */

    @POST
    @Path("/jobs/{jobs}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some job attributes", response = Job.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response jobUpdate(
            @ApiParam(value = ParamConstants.JOBS_DESCRIPTION, required = true) @PathParam("jobs") String jobStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) Boolean includeResult,
            @ApiParam(value = "body") JobUpdateParams parameters) {
        return run(() -> jobManager.update(studyStr, getIdList(jobStr), parameters, queryOptions, token));
    }


    @GET
    @Path("/jobs/{jobs}/info")
    @ApiOperation(value = "Get job information", response = Job.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response jobInfo(
            @ApiParam(value = ParamConstants.JOBS_DESCRIPTION, required = true) @PathParam("jobs") String jobIds,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Boolean to retrieve deleted jobs", defaultValue = "false") @QueryParam("deleted") boolean deleted) {
        return run(() -> {
            List<String> idList = getIdList(jobIds);
            return jobManager.get(studyStr, idList, new Query("deleted", deleted), queryOptions, true, token);
        });
    }


    @GET
    @Path("/jobs/search")
    @ApiOperation(value = "Job search method", response = Job.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response jobSearch(
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
            @ApiParam(value = ParamConstants.EXECUTION_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.JOB_INPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_INPUT_FILES_PARAM) String input,
            @ApiParam(value = ParamConstants.JOB_OUTPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_OUTPUT_FILES_PARAM) String output,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam("deleted") boolean deleted) {
        return run(() -> {
            query.remove(ParamConstants.STUDY_PARAM);
            return jobManager.search(studyStr, query, queryOptions, token);
        });
    }


    @GET
    @Path("/jobs/distinct")
    @ApiOperation(value = "Job distinct method", response = Object.class)
    public Response jobDistinct(
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
            @ApiParam(value = ParamConstants.EXECUTION_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.JOB_INPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_INPUT_FILES_PARAM) String input,
            @ApiParam(value = ParamConstants.JOB_OUTPUT_FILES_DESCRIPTION) @QueryParam(ParamConstants.JOB_OUTPUT_FILES_PARAM) String output,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        return run(() -> {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);
            return jobManager.distinct(studyStr, field, query, token);
        });
    }


    @GET
    @Path("/jobs/aggregationStats")
    @ApiOperation(value = "Fetch catalog job stats", response = FacetField.class)
    public Response jobAggregationStats(
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
        return run(() -> {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            return catalogManager.getJobManager().facet(studyStr, query, queryOptions, defaultStats, token);
        });
    }


    @GET
    @Path("/jobs/{job}/log/head")
    @ApiOperation(value = "Show the first lines of a log file (up to a limit)", response = FileContent.class)
    public Response jobLogHead(
            @ApiParam(value = ParamConstants.JOB_ID_DESCRIPTION, required = true) @PathParam("job") String jobId,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Starting byte from which the file will be read") @QueryParam("offset") long offset,
            @ApiParam(value = ParamConstants.MAXIMUM_LINES_CONTENT_DESCRIPTION, defaultValue = "20") @QueryParam("lines") Integer lines,
            @ApiParam(value = "Log file to be shown (stdout or stderr)") @DefaultValue("stderr") @QueryParam("type") String type) {
        return run(() -> {
            int nlines = lines != null ? lines : 20;
            ParamUtils.checkIsSingleID(jobId);
            return catalogManager.getJobManager().log(studyStr, jobId, offset, nlines, type, false, token);
        });
    }


    @GET
    @Path("/jobs/{job}/log/tail")
    @ApiOperation(value = "Show the last lines of a log file (up to a limit)", response = FileContent.class)
    public Response jobLogTail(
            @ApiParam(value = ParamConstants.JOB_ID_DESCRIPTION, required = true) @PathParam("job") String jobId,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.MAXIMUM_LINES_CONTENT_DESCRIPTION, defaultValue = "20") @QueryParam("lines") Integer lines,
            @ApiParam(value = "Log file to be shown (stdout or stderr)") @DefaultValue("stderr") @QueryParam("type") String type) {
        return run(() -> {
            int nlines = lines != null ? lines : 20;
            ParamUtils.checkIsSingleID(jobId);
            return catalogManager.getJobManager().log(studyStr, jobId, 0, nlines, type, true, token);
        });
    }
}
