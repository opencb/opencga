package org.opencb.opencga.server.rest;

import org.apache.commons.lang3.ObjectUtils;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.workflow.NextFlowExecutor;
import org.opencb.opencga.catalog.managers.WorkflowManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.workflow.*;
import org.opencb.opencga.core.tools.annotations.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.JOB_DEPENDS_ON;

@Path("/{apiVersion}/workflows")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Workflows", description = "Methods for working with 'workflows' endpoint")
public class WorkflowWSServer extends OpenCGAWSServer {

    private WorkflowManager workflowManager;

    public WorkflowWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        workflowManager = catalogManager.getWorkflowManager();
    }

    @GET
    @Path("/{workflows}/info")
    @ApiOperation(value = "Get workflow information", response = Workflow.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, format = "", example = "name,attributes",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType =
                    "string", paramType = "query")
    })
    public Response workflowInfo(
            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION, required = true) @PathParam("workflows") String workflowStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.WORKFLOW_VERSION_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOW_VERSION_PARAM) String version,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);

            List<String> workflowList = getIdList(workflowStr);
            DataResult<Workflow> workflowDataResult = workflowManager.get(studyStr, workflowList, query, queryOptions, true, token);
            return createOkResponse(workflowDataResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create a workflow", response = Workflow.class, notes = "Create a workflow.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response createWorkflowPOST(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing workflow information", required = true) WorkflowCreateParams params) {
        try {
            params = ObjectUtils.defaultIfNull(params, new WorkflowCreateParams());

            Workflow workflow = params.toWorkflow();

            return createOkResponse(workflowManager.create(studyStr, workflow, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/run")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = NextFlowExecutor.DESCRIPTION, response = Job.class)
    public Response updateByPost(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.JOB_SCHEDULED_START_TIME_DESCRIPTION) @QueryParam(ParamConstants.JOB_SCHEDULED_START_TIME) String scheduledStartTime,
            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.SUBMIT_JOB_PRIORITY_PARAM) String jobPriority,
            @ApiParam(value = ParamConstants.JOB_DRY_RUN_DESCRIPTION) @QueryParam(ParamConstants.JOB_DRY_RUN) Boolean dryRun,
            @ApiParam(value = NextFlowRunParams.DESCRIPTION, required = true) NextFlowRunParams params) {
        return submitJob(NextFlowExecutor.ID, study, params, jobName, jobDescription, dependsOn, jobTags, scheduledStartTime, jobPriority, dryRun);
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Workflow search method", response = Workflow.class)
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
            @ApiParam(value = ParamConstants.WORKFLOWS_ID_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.WORKFLOWS_NAME_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.WORKFLOWS_UUID_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.WORKFLOWS_TAGS_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.WORKFLOWS_DRAFT_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_DRAFT_PARAM) Boolean draft,
            @ApiParam(value = ParamConstants.WORKFLOWS_INTERNAL_REGISTRATION_USER_ID_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_INTERNAL_REGISTRATION_USER_ID_PARAM) String userId,
            @ApiParam(value = ParamConstants.WORKFLOWS_TYPE_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_TYPE_PARAM) String type,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.SNAPSHOT_PARAM) int snapshot,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted
    ) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(workflowManager.search(studyStr, query, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/distinct")
    @ApiOperation(value = "Workflow distinct method", response = Object.class)
    public Response distinct(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.WORKFLOWS_ID_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.WORKFLOWS_NAME_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.WORKFLOWS_UUID_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.WORKFLOWS_TAGS_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.WORKFLOWS_DRAFT_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_DRAFT_PARAM) Boolean draft,
            @ApiParam(value = ParamConstants.WORKFLOWS_INTERNAL_REGISTRATION_USER_ID_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_INTERNAL_REGISTRATION_USER_ID_PARAM) String userId,
            @ApiParam(value = ParamConstants.WORKFLOWS_TYPE_DESCRIPTION) @QueryParam(ParamConstants.WORKFLOWS_TYPE_PARAM) String type,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.SNAPSHOT_PARAM) int snapshot,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);
            List<String> fields = split(field, ParamConstants.DISTINCT_FIELD_PARAM, true);
            return createOkResponse(workflowManager.distinct(studyStr, fields, query, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{workflowId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some workflow attributes", response = Workflow.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response updateByPost(
            @ApiParam(value = ParamConstants.WORKFLOWS_DESCRIPTION, required = true) @PathParam("workflowId") String workflowId,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
//            @ApiParam(value = ParamConstants.WORKFLOW_SCRIPTS_ACTION_DESCRIPTION, allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD") @QueryParam(ParamConstants.WORKFLOW_SCRIPTS_ACTION_PARAM) ParamUtils.BasicUpdateAction workflowScriptsAction,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "body") WorkflowUpdateParams parameters) {
        try {
//            Map<String, Object> actionMap = new HashMap<>();
//            queryOptions.put(Constants.ACTIONS, actionMap);

            return createOkResponse(workflowManager.update(studyStr, workflowId, parameters, queryOptions, token), "Workflow update success");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/{workflows}/delete")
    @ApiOperation(value = "Delete workflows", response = Workflow.class)
    public Response delete(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.WORKFLOWS_DESCRIPTION) @PathParam("workflows") String workflows) {
        try {
            return createOkResponse(workflowManager.delete(studyStr, getIdList(workflows), queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{workflows}/acl")
    @ApiOperation(value = "Returns the acl of the workflows. If member is provided, it will only return the acl for the member.",
            response = WorkflowAclEntryList.class)
    public Response getAcls(@ApiParam(value = ParamConstants.WORKFLOWS_DESCRIPTION, required = true) @PathParam("workflows") String workflowIdsStr,
                            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION, defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        try {
            List<String> idList = getIdList(workflowIdsStr);
            return createOkResponse(workflowManager.getAcls(studyStr, idList, member, silent, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @POST
//    @Path("/acl/{members}/update")
//    @ApiOperation(value = "Update the set of permissions granted for the member", response = WorkflowAclEntryList.class)
//    public Response updateAcl(
//            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
//            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
//            @ApiParam(value = ParamConstants.ACL_ACTION_DESCRIPTION, required = true, defaultValue = "ADD") @QueryParam(ParamConstants.ACL_ACTION_PARAM) ParamUtils.AclAction action,
//            @ApiParam(value = "JSON containing the parameters to update the permissions. If propagate flag is set to true, it will "
//                    + "propagate the permissions defined to the individuals that are associated to the matching workflows", required = true)
//            SampleAclUpdateParams params) {
//        try {
//            params = ObjectUtils.defaultIfNull(params, new SampleAclUpdateParams());
//            SampleAclParams sampleAclParams = new SampleAclParams(
//                    params.getIndividual(), params.getFamily(), params.getFile(), params.getCohort(), params.getPermissions());
//            List<String> idList = StringUtils.isEmpty(params.getSample()) ? Collections.emptyList() : getIdList(params.getSample(), false);
//            return createOkResponse(sampleManager.updateAcl(studyStr, idList, memberId, sampleAclParams, action, token));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }


}
