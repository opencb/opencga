package org.opencb.opencga.app.cli.main.executors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.main.*;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.WorkflowsCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.utils.ParamUtils.AclAction;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.workflow.NextFlowRunParams;
import org.opencb.opencga.core.models.workflow.Workflow;
import org.opencb.opencga.core.models.workflow.WorkflowAclEntryList;
import org.opencb.opencga.core.models.workflow.WorkflowAclUpdateParams;
import org.opencb.opencga.core.models.workflow.WorkflowCreateParams;
import org.opencb.opencga.core.models.workflow.WorkflowMinimumRequirements;
import org.opencb.opencga.core.models.workflow.WorkflowRepository;
import org.opencb.opencga.core.models.workflow.WorkflowRepositoryParams;
import org.opencb.opencga.core.models.workflow.WorkflowSystem;
import org.opencb.opencga.core.models.workflow.WorkflowUpdateParams;
import org.opencb.opencga.core.response.QueryType;
import org.opencb.opencga.core.response.RestResponse;


/*
* WARNING: AUTOGENERATED CODE
*
* This code was generated by a tool.
*
* Manual changes to this file may cause unexpected behavior in your application.
* Manual changes to this file will be overwritten if the code is regenerated.
*  
*/
/**
 * This class contains methods for the Workflows command line.
 *    PATH: /{apiVersion}/workflows
 */
public class WorkflowsCommandExecutor extends OpencgaCommandExecutor {

    public String categoryName = "workflows";
    public WorkflowsCommandOptions workflowsCommandOptions;

    public WorkflowsCommandExecutor(WorkflowsCommandOptions workflowsCommandOptions) throws CatalogAuthenticationException {
        super(workflowsCommandOptions.commonCommandOptions);
        this.workflowsCommandOptions = workflowsCommandOptions;
    }

    @Override
    public void execute() throws Exception {

        logger.debug("Executing Workflows command line");

        String subCommandString = getParsedSubCommand(workflowsCommandOptions.jCommander);

        RestResponse queryResponse = null;

        switch (subCommandString) {
            case "acl-update":
                queryResponse = updateAcl();
                break;
            case "create":
                queryResponse = create();
                break;
            case "distinct":
                queryResponse = distinct();
                break;
            case "import":
                queryResponse = importWorkflow();
                break;
            case "run":
                queryResponse = run();
                break;
            case "search":
                queryResponse = search();
                break;
            case "update":
                queryResponse = update();
                break;
            case "acl":
                queryResponse = acl();
                break;
            case "delete":
                queryResponse = delete();
                break;
            case "info":
                queryResponse = info();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);

    }

    private RestResponse<WorkflowAclEntryList> updateAcl() throws Exception {
        logger.debug("Executing updateAcl in Workflows command line");

        WorkflowsCommandOptions.UpdateAclCommandOptions commandOptions = workflowsCommandOptions.updateAclCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        WorkflowAclUpdateParams workflowAclUpdateParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<WorkflowAclEntryList> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/workflows/acl/{members}/update"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            workflowAclUpdateParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), WorkflowAclUpdateParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotNull(beanParams, "workflowIds", commandOptions.workflowIds, true);
            putNestedIfNotNull(beanParams, "permissions", commandOptions.permissions, true);

            workflowAclUpdateParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), WorkflowAclUpdateParams.class);
        }
        return openCGAClient.getWorkflowClient().updateAcl(commandOptions.members, commandOptions.action, workflowAclUpdateParams, queryParams);
    }

    private RestResponse<Workflow> create() throws Exception {
        logger.debug("Executing create in Workflows command line");

        WorkflowsCommandOptions.CreateCommandOptions commandOptions = workflowsCommandOptions.createCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotNull("includeResult", commandOptions.includeResult);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        WorkflowCreateParams workflowCreateParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Workflow> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/workflows/create"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            workflowCreateParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), WorkflowCreateParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "id", commandOptions.id, true);
            putNestedIfNotEmpty(beanParams, "name", commandOptions.name, true);
            putNestedIfNotEmpty(beanParams, "description", commandOptions.description, true);
            putNestedIfNotNull(beanParams, "manager.id", commandOptions.managerId, true);
            putNestedIfNotEmpty(beanParams, "manager.version", commandOptions.managerVersion, true);
            putNestedIfNotNull(beanParams, "type", commandOptions.type, true);
            putNestedIfNotNull(beanParams, "tags", commandOptions.tags, true);
            putNestedIfNotNull(beanParams, "draft", commandOptions.draft, true);
            putNestedIfNotEmpty(beanParams, "repository.id", commandOptions.repositoryId, true);
            putNestedIfNotEmpty(beanParams, "repository.version", commandOptions.repositoryVersion, true);
            putNestedIfNotEmpty(beanParams, "repository.author", commandOptions.repositoryAuthor, true);
            putNestedIfNotEmpty(beanParams, "repository.description", commandOptions.repositoryDescription, true);
            putNestedIfNotNull(beanParams, "minimumRequirements.cpu", commandOptions.minimumRequirementsCpu, true);
            putNestedIfNotNull(beanParams, "minimumRequirements.memory", commandOptions.minimumRequirementsMemory, true);
            putNestedIfNotNull(beanParams, "minimumRequirements.disk", commandOptions.minimumRequirementsDisk, true);
            putNestedIfNotEmpty(beanParams, "creationDate", commandOptions.creationDate, true);
            putNestedIfNotEmpty(beanParams, "modificationDate", commandOptions.modificationDate, true);
            putNestedMapIfNotEmpty(beanParams, "attributes", commandOptions.attributes, true);

            workflowCreateParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), WorkflowCreateParams.class);
        }
        return openCGAClient.getWorkflowClient().create(workflowCreateParams, queryParams);
    }

    private RestResponse<Object> distinct() throws Exception {
        logger.debug("Executing distinct in Workflows command line");

        WorkflowsCommandOptions.DistinctCommandOptions commandOptions = workflowsCommandOptions.distinctCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("id", commandOptions.id);
        queryParams.putIfNotEmpty("name", commandOptions.name);
        queryParams.putIfNotEmpty("uuid", commandOptions.uuid);
        queryParams.putIfNotEmpty("tags", commandOptions.tags);
        queryParams.putIfNotNull("draft", commandOptions.draft);
        queryParams.putIfNotEmpty("internalRegistrationUserId", commandOptions.internalRegistrationUserId);
        queryParams.putIfNotEmpty("managerId", commandOptions.managerId);
        queryParams.putIfNotEmpty("type", commandOptions.type);
        queryParams.putIfNotEmpty("creationDate", commandOptions.creationDate);
        queryParams.putIfNotEmpty("modificationDate", commandOptions.modificationDate);
        queryParams.putIfNotEmpty("acl", commandOptions.acl);
        queryParams.putIfNotEmpty("release", commandOptions.release);
        queryParams.putIfNotNull("snapshot", commandOptions.snapshot);
        queryParams.putIfNotNull("deleted", commandOptions.deleted);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getWorkflowClient().distinct(commandOptions.field, queryParams);
    }

    private RestResponse<Job> importWorkflow() throws Exception {
        logger.debug("Executing importWorkflow in Workflows command line");

        WorkflowsCommandOptions.ImportCommandOptions commandOptions = workflowsCommandOptions.importCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        WorkflowRepositoryParams workflowRepositoryParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Job> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/workflows/import"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            workflowRepositoryParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), WorkflowRepositoryParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "id", commandOptions.id, true);
            putNestedIfNotEmpty(beanParams, "version", commandOptions.version, true);

            workflowRepositoryParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), WorkflowRepositoryParams.class);
        }
        return openCGAClient.getWorkflowClient().importWorkflow(workflowRepositoryParams, queryParams);
    }

    private RestResponse<Job> run() throws Exception {
        logger.debug("Executing run in Workflows command line");

        WorkflowsCommandOptions.RunCommandOptions commandOptions = workflowsCommandOptions.runCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("jobId", commandOptions.jobId);
        queryParams.putIfNotEmpty("jobDescription", commandOptions.jobDescription);
        queryParams.putIfNotEmpty("jobDependsOn", commandOptions.jobDependsOn);
        queryParams.putIfNotEmpty("jobTags", commandOptions.jobTags);
        queryParams.putIfNotEmpty("jobScheduledStartTime", commandOptions.jobScheduledStartTime);
        queryParams.putIfNotEmpty("jobPriority", commandOptions.jobPriority);
        queryParams.putIfNotNull("jobDryRun", commandOptions.jobDryRun);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        NextFlowRunParams nextFlowRunParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Job> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/workflows/run"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            nextFlowRunParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), NextFlowRunParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "id", commandOptions.id, true);
            putNestedIfNotNull(beanParams, "version", commandOptions.version, true);
            putNestedMapIfNotEmpty(beanParams, "params", commandOptions.params, true);

            nextFlowRunParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), NextFlowRunParams.class);
        }
        return openCGAClient.getWorkflowClient().run(nextFlowRunParams, queryParams);
    }

    private RestResponse<Workflow> search() throws Exception {
        logger.debug("Executing search in Workflows command line");

        WorkflowsCommandOptions.SearchCommandOptions commandOptions = workflowsCommandOptions.searchCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotNull("limit", commandOptions.limit);
        queryParams.putIfNotNull("skip", commandOptions.skip);
        queryParams.putIfNotNull("count", commandOptions.count);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("id", commandOptions.id);
        queryParams.putIfNotEmpty("name", commandOptions.name);
        queryParams.putIfNotEmpty("uuid", commandOptions.uuid);
        queryParams.putIfNotEmpty("tags", commandOptions.tags);
        queryParams.putIfNotNull("draft", commandOptions.draft);
        queryParams.putIfNotEmpty("internalRegistrationUserId", commandOptions.internalRegistrationUserId);
        queryParams.putIfNotEmpty("managerId", commandOptions.managerId);
        queryParams.putIfNotEmpty("type", commandOptions.type);
        queryParams.putIfNotEmpty("creationDate", commandOptions.creationDate);
        queryParams.putIfNotEmpty("modificationDate", commandOptions.modificationDate);
        queryParams.putIfNotEmpty("acl", commandOptions.acl);
        queryParams.putIfNotEmpty("release", commandOptions.release);
        queryParams.putIfNotNull("snapshot", commandOptions.snapshot);
        queryParams.putIfNotNull("deleted", commandOptions.deleted);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getWorkflowClient().search(queryParams);
    }

    private RestResponse<Workflow> update() throws Exception {
        logger.debug("Executing update in Workflows command line");

        WorkflowsCommandOptions.UpdateCommandOptions commandOptions = workflowsCommandOptions.updateCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotNull("includeResult", commandOptions.includeResult);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        WorkflowUpdateParams workflowUpdateParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Workflow> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/workflows/{workflowId}/update"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            workflowUpdateParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), WorkflowUpdateParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "name", commandOptions.name, true);
            putNestedIfNotEmpty(beanParams, "description", commandOptions.description, true);
            putNestedIfNotNull(beanParams, "manager.id", commandOptions.managerId, true);
            putNestedIfNotEmpty(beanParams, "manager.version", commandOptions.managerVersion, true);
            putNestedIfNotNull(beanParams, "type", commandOptions.type, true);
            putNestedIfNotNull(beanParams, "tags", commandOptions.tags, true);
            putNestedIfNotNull(beanParams, "draft", commandOptions.draft, true);
            putNestedIfNotEmpty(beanParams, "repository.id", commandOptions.repositoryId, true);
            putNestedIfNotEmpty(beanParams, "repository.version", commandOptions.repositoryVersion, true);
            putNestedIfNotEmpty(beanParams, "repository.author", commandOptions.repositoryAuthor, true);
            putNestedIfNotEmpty(beanParams, "repository.description", commandOptions.repositoryDescription, true);
            putNestedIfNotEmpty(beanParams, "creationDate", commandOptions.creationDate, true);
            putNestedIfNotEmpty(beanParams, "modificationDate", commandOptions.modificationDate, true);
            putNestedMapIfNotEmpty(beanParams, "attributes", commandOptions.attributes, true);

            workflowUpdateParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), WorkflowUpdateParams.class);
        }
        return openCGAClient.getWorkflowClient().update(commandOptions.workflowId, workflowUpdateParams, queryParams);
    }

    private RestResponse<WorkflowAclEntryList> acl() throws Exception {
        logger.debug("Executing acl in Workflows command line");

        WorkflowsCommandOptions.AclCommandOptions commandOptions = workflowsCommandOptions.aclCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("member", commandOptions.member);
        queryParams.putIfNotNull("silent", commandOptions.silent);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getWorkflowClient().acl(commandOptions.workflows, queryParams);
    }

    private RestResponse<Workflow> delete() throws Exception {
        logger.debug("Executing delete in Workflows command line");

        WorkflowsCommandOptions.DeleteCommandOptions commandOptions = workflowsCommandOptions.deleteCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getWorkflowClient().delete(commandOptions.workflows, queryParams);
    }

    private RestResponse<Workflow> info() throws Exception {
        logger.debug("Executing info in Workflows command line");

        WorkflowsCommandOptions.InfoCommandOptions commandOptions = workflowsCommandOptions.infoCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("version", commandOptions.version);
        queryParams.putIfNotNull("deleted", commandOptions.deleted);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getWorkflowClient().info(commandOptions.workflows, queryParams);
    }
}