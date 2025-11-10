package org.opencb.opencga.server.rest;

import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.customTool.CustomToolBuilder;
import org.opencb.opencga.analysis.customTool.CustomToolExecutor;
import org.opencb.opencga.catalog.managers.ExternalToolManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.externalTool.*;
import org.opencb.opencga.core.models.externalTool.custom.*;
import org.opencb.opencga.core.models.externalTool.workflow.WorkflowCreateParams;
import org.opencb.opencga.core.models.externalTool.workflow.WorkflowUpdateParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobToolBuildParams;
import org.opencb.opencga.core.models.job.JobType;
import org.opencb.opencga.core.models.job.ToolInfo;
import org.opencb.opencga.core.models.variant.VariantWalkerParams;
import org.opencb.opencga.core.tools.annotations.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.JOB_DEPENDS_ON;

@Path("/{apiVersion}/tools")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "User Tools", description = "Methods for working with 'tools' endpoint")
public class ExternalToolWSServer extends OpenCGAWSServer {

    private ExternalToolManager externalToolManager;

    public ExternalToolWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        externalToolManager = catalogManager.getExternalToolManager();
    }

    @GET
    @Path("/{tools}/info")
    @ApiOperation(value = "Get user tool information", response = ExternalTool.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, format = "", example = "name,attributes",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType =
                    "string", paramType = "query")
    })
    public Response info(
            @ApiParam(value = ParamConstants.EXTERNAL_TOOLS_DESCRIPTION, required = true) @PathParam("tools") String toolsStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_VERSION_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_VERSION_PARAM) String version,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted) {
        return run(() -> {
            query.remove(ParamConstants.STUDY_PARAM);
            List<String> toolList = getIdList(toolsStr);
            return externalToolManager.get(studyStr, toolList, query, queryOptions, true, token);
        });
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "User tool search method", response = ExternalTool.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType =
                    "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType =
                    "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType =
                    "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_ID_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_NAME_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_UUID_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_TAGS_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_DRAFT_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_DRAFT_PARAM) Boolean draft,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_INTERNAL_REGISTRATION_USER_ID_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_INTERNAL_REGISTRATION_USER_ID_PARAM) String userId,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_TYPE_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_TYPE_PARAM) String type,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_SCOPE_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_SCOPE_PARAM) String scope,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_WORKFLOW_REPOSITORY_NAME_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_WORKFLOW_REPOSITORY_NAME_PARAM) String repositoryName,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_DOCKER_NAME_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_DOCKER_NAME_PARAM) String dockerName,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.SNAPSHOT_PARAM) int snapshot,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted
    ) {
        return run(() -> {
            query.remove(ParamConstants.STUDY_PARAM);
            return externalToolManager.search(studyStr, query, queryOptions, token);
        });
    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Fetch user tool stats", response = FacetField.class)
    public Response getAggregationStats(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_ID_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_NAME_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_UUID_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_TAGS_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_DRAFT_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_DRAFT_PARAM) Boolean draft,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_INTERNAL_REGISTRATION_USER_ID_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_INTERNAL_REGISTRATION_USER_ID_PARAM) String userId,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_TYPE_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_TYPE_PARAM) String type,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_SCOPE_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_SCOPE_PARAM) String scope,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_WORKFLOW_REPOSITORY_NAME_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_WORKFLOW_REPOSITORY_NAME_PARAM) String repositoryName,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_DOCKER_NAME_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_DOCKER_NAME_PARAM) String dockerName,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.SNAPSHOT_PARAM) int snapshot,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted,

            // Facet field
            @ApiParam(value = ParamConstants.FACET_DESCRIPTION) @QueryParam(ParamConstants.FACET_PARAM) String facet) {
        return run(() -> {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.FACET_PARAM);
            return catalogManager.getExternalToolManager().facet(studyStr, query, facet, token);
        });
    }

    @GET
    @Path("/distinct")
    @ApiOperation(value = "User tool distinct method", response = Object.class)
    public Response distinct(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_ID_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_NAME_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_UUID_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_TAGS_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_DRAFT_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_DRAFT_PARAM) Boolean draft,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_INTERNAL_REGISTRATION_USER_ID_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_INTERNAL_REGISTRATION_USER_ID_PARAM) String userId,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_TYPE_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_TYPE_PARAM) String type,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_SCOPE_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_SCOPE_PARAM) String scope,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_WORKFLOW_REPOSITORY_NAME_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_WORKFLOW_REPOSITORY_NAME_PARAM) String repositoryName,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_DOCKER_NAME_DESCRIPTION) @QueryParam(ParamConstants.EXTERNAL_TOOL_DOCKER_NAME_PARAM) String dockerName,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.SNAPSHOT_PARAM) int snapshot,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        return run(() -> {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);
            List<String> fields = split(field, ParamConstants.DISTINCT_FIELD_PARAM, true);
            return externalToolManager.distinct(studyStr, fields, query, token);
        });
    }

    @GET
    @Path("/{tools}/acl")
    @ApiOperation(value = "Returns the acl of the user tools. If member is provided, it will only return the acl for the member.",
            response = ExternalToolAclEntryList.class)
    public Response getAcls(@ApiParam(value = ParamConstants.EXTERNAL_TOOLS_DESCRIPTION, required = true) @PathParam("tools") String toolsStr,
                            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION, defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        return run(() -> {
            List<String> idList = getIdList(toolsStr);
            return externalToolManager.getAcls(studyStr, idList, member, silent, token);
        });
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of user tool permissions granted for the member", response = ExternalToolAclEntryList.class)
    public Response updateAcl(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberIds,
            @ApiParam(value = ParamConstants.ACL_ACTION_DESCRIPTION, required = true, defaultValue = "ADD")
            @QueryParam(ParamConstants.ACL_ACTION_PARAM) ParamUtils.AclAction action,
            @ApiParam(value = "JSON containing the parameters to update the permissions.", required = true) ExternalToolAclUpdateParams params) {
        return run(() -> externalToolManager.updateAcl(studyStr, memberIds, params, action, token));
    }

    @DELETE
    @Path("/{tools}/delete")
    @ApiOperation(value = "Delete user tools", response = ExternalTool.class)
    public Response delete(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.EXTERNAL_TOOLS_DESCRIPTION) @PathParam("tools") String toolsStr) {
        return run(() -> externalToolManager.delete(studyStr, getIdList(toolsStr), queryOptions, token));
    }

    // ********************************** CUSTOM TOOL WS ENDPOINTS **********************************

    @POST
    @Path("/custom/builder/run")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = CustomToolExecutor.DESCRIPTION, response = Job.class)
    public Response dockerBuildByPost(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.JOB_SCHEDULED_START_TIME_DESCRIPTION) @QueryParam(ParamConstants.JOB_SCHEDULED_START_TIME) String scheduledStartTime,
            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.SUBMIT_JOB_PRIORITY_PARAM) String jobPriority,
            @ApiParam(value = ParamConstants.JOB_DRY_RUN_DESCRIPTION) @QueryParam(ParamConstants.JOB_DRY_RUN) Boolean dryRun,
            @ApiParam(value = "body", required = true) JobToolBuildParams params) {
        return submitJob(study, JobType.NATIVE_TOOL, CustomToolBuilder.ID, params, jobName, jobDescription, dependsOn, jobTags, scheduledStartTime, jobPriority, dryRun);
    }

    @POST
    @Path("/custom/create")
    @ApiOperation(value = "Register a new user tool of type CUSTOM_TOOL", response = ExternalTool.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response createCustomTool(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing workflow information", required = true) CustomToolCreateParams toolCreateParams) {
        return run(() -> externalToolManager.createCustomTool(studyStr, toolCreateParams, queryOptions, token));
    }

    @POST
    @Path("/custom/{toolId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some custom user tool attributes", response = ExternalTool.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response updateCustomTool(
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_ID_DESCRIPTION, required = true) @PathParam("toolId") String toolId,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "body") CustomToolUpdateParams parameters) {
        return run(() -> externalToolManager.updateCustomTool(studyStr, toolId, parameters, queryOptions, token));
    }

    @POST
    @Path("/custom/run")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = CustomToolExecutor.DESCRIPTION, response = Job.class)
    public Response runCustomTool(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.JOB_SCHEDULED_START_TIME_DESCRIPTION) @QueryParam(ParamConstants.JOB_SCHEDULED_START_TIME) String scheduledStartTime,
            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.SUBMIT_JOB_PRIORITY_PARAM) String jobPriority,
            @ApiParam(value = ParamConstants.JOB_DRY_RUN_DESCRIPTION) @QueryParam(ParamConstants.JOB_DRY_RUN) Boolean dryRun,
            @ApiParam(value = "Container image to be executed and its requirements", required = true) CustomToolInlineParams params) {
        try {
            CustomToolParams runParams = new CustomToolParams(params.getContainer());
            ToolInfo toolInfo = new ToolInfo()
                    .setId(params.getContainer().getName())
                    .setMinimumRequirements(params.getMinimumRequirements());
            return submitJob(study, JobType.CUSTOM_TOOL, toolInfo, runParams, jobName, jobDescription, dependsOn, jobTags, scheduledStartTime,
                    jobPriority, dryRun);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/custom/{toolId}/run")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = CustomToolExecutor.DESCRIPTION, response = Job.class)
    public Response runCustomToolByToolId(
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_ID_DESCRIPTION, required = true) @PathParam("toolId") String toolId,
            @ApiParam(value = "Tool version. If not provided, the latest version will be used.") @QueryParam(ParamConstants.EXTERNAL_TOOL_VERSION_PARAM) Integer version,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.JOB_SCHEDULED_START_TIME_DESCRIPTION) @QueryParam(ParamConstants.JOB_SCHEDULED_START_TIME) String scheduledStartTime,
            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.SUBMIT_JOB_PRIORITY_PARAM) String jobPriority,
            @ApiParam(value = ParamConstants.JOB_DRY_RUN_DESCRIPTION) @QueryParam(ParamConstants.JOB_DRY_RUN) Boolean dryRun,
            @ApiParam(value = CustomToolRunParams.DESCRIPTION, required = true) CustomToolRunParams params) {
        return run(() -> catalogManager.getExternalToolManager().submitCustomTool(study, toolId, version, params, jobName, jobDescription,
                dependsOn, jobTags, scheduledStartTime, jobPriority, dryRun, token));
    }

    // ********************************** WORKFLOW WS ENDPOINTS **********************************

    @POST
    @Path("/workflow/create")
    @ApiOperation(value = "Register a new user tool of type WORKFLOW", response = ExternalTool.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response createWorkflow(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing workflow information", required = true) WorkflowCreateParams workflow) {
        return run(() -> externalToolManager.createWorkflow(studyStr, workflow, queryOptions, token));
    }

    @POST
    @Path("/workflow/{toolId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some user tool attributes", response = ExternalTool.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response updateWorkflow(
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_ID_DESCRIPTION, required = true) @PathParam("toolId") String toolId,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "body") WorkflowUpdateParams parameters) {
        try {
            return createOkResponse(externalToolManager.updateWorkflow(studyStr, toolId, parameters, queryOptions, token), "Workflow update success");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/workflow/import")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Import a user tool of type WORKFLOW", response = ExternalTool.class)
    public Response importWorkflow(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = "Repository parameters", required = true) WorkflowRepositoryParams params) {
        return run(() -> catalogManager.getExternalToolManager().importWorkflow(study, params, queryOptions, token));
    }

    @POST
    @Path("/workflow/{toolId}/run")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Execute a user tool of type WORKFLOW", response = Job.class)
    public Response executeWorkflow(
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_ID_DESCRIPTION, required = true) @PathParam("toolId") String toolId,
            @ApiParam(value = "Tool version. If not provided, the latest version will be used.") @QueryParam(ParamConstants.EXTERNAL_TOOL_VERSION_PARAM) Integer version,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.JOB_SCHEDULED_START_TIME_DESCRIPTION) @QueryParam(ParamConstants.JOB_SCHEDULED_START_TIME) String scheduledStartTime,
            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.SUBMIT_JOB_PRIORITY_PARAM) String jobPriority,
            @ApiParam(value = ParamConstants.JOB_DRY_RUN_DESCRIPTION) @QueryParam(ParamConstants.JOB_DRY_RUN) Boolean dryRun,
            @ApiParam(value = WorkflowParams.DESCRIPTION, required = true) WorkflowParams params) {
        return run(() -> catalogManager.getExternalToolManager().submitWorkflow(study, toolId, version, params, jobName, jobDescription,
                dependsOn, jobTags, scheduledStartTime, jobPriority, dryRun, token));
    }

    // ********************************** VARIANT WALKER WS ENDPOINTS **********************************

    @POST
    @Path("/walker/create")
    @ApiOperation(value = "Register a new user tool of type VARIANT_WALKER", response = ExternalTool.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response createVariantWalkerTool(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing workflow information", required = true) CustomToolCreateParams toolCreateParams) {
        return run(() -> externalToolManager.createVariantWalkerTool(studyStr, toolCreateParams, queryOptions, token));
    }

    @POST
    @Path("/walker/{toolId}/run")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = CustomToolExecutor.DESCRIPTION, response = Job.class)
    public Response runWalkerByToolId(
            @ApiParam(value = ParamConstants.EXTERNAL_TOOL_ID_DESCRIPTION, required = true) @PathParam("toolId") String toolId,
            @ApiParam(value = "Tool version. If not provided, the latest version will be used.") @QueryParam(ParamConstants.EXTERNAL_TOOL_VERSION_PARAM) Integer version,
            @ApiParam(value = ParamConstants.PROJECT_DESCRIPTION) @QueryParam(ParamConstants.PROJECT_PARAM) String project,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.JOB_SCHEDULED_START_TIME_DESCRIPTION) @QueryParam(ParamConstants.JOB_SCHEDULED_START_TIME) String scheduledStartTime,
            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.SUBMIT_JOB_PRIORITY_PARAM) String jobPriority,
            @ApiParam(value = ParamConstants.JOB_DRY_RUN_DESCRIPTION) @QueryParam(ParamConstants.JOB_DRY_RUN) Boolean dryRun,
            @ApiParam(value = VariantWalkerParams.DESCRIPTION, required = true) VariantWalkerParams params) {
        return run(() -> catalogManager.getExternalToolManager().submitVariantWalker(project, study, toolId, version, params, jobName, jobDescription,
                dependsOn, jobTags, scheduledStartTime, jobPriority, dryRun, token));
    }
///tools/walker/{build ?? | create | update}
///tools/walker/run x2?


}
