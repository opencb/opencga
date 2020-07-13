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

package org.opencb.opencga.app.cli.main.executors.catalog;


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.JobCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobAclUpdateParams;
import org.opencb.opencga.core.models.job.JobCreateParams;
import org.opencb.opencga.core.models.job.ToolInfo;
import org.opencb.opencga.core.response.RestResponse;

import java.util.stream.Collectors;

/**
 * Created by imedina on 03/06/16.
 */
public class JobCommandExecutor extends OpencgaCommandExecutor {

    private JobCommandOptions jobsCommandOptions;

    public JobCommandExecutor(JobCommandOptions jobsCommandOptions) {
        super(jobsCommandOptions.commonCommandOptions);
        this.jobsCommandOptions = jobsCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing jobs command line");

        String subCommandString = getParsedSubCommand(jobsCommandOptions.jCommander);
        RestResponse queryResponse = null;
        switch (subCommandString) {
            case "create":
                queryResponse = create();
                break;
            case "info":
                queryResponse = info();
                break;
            case "search":
                queryResponse = search();
                break;
            case "top":
                top();
                break;
            case "log":
                log();
                break;
            case "delete":
                queryResponse = delete();
                break;
            case "acl":
                queryResponse = acl();
                break;
            case "acl-update":
                queryResponse = updateAcl();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    private RestResponse<Job> create() throws ClientException {
        logger.debug("Creating a new job");

        JobCommandOptions.CreateCommandOptions commandOptions = jobsCommandOptions.createCommandOptions;

        JobCreateParams createParams = new JobCreateParams()
                .setId(commandOptions.id)
                .setDescription(commandOptions.description)
                .setCommandLine(commandOptions.commandLine)
                .setPriority(commandOptions.priority)
                .setCreationDate(commandOptions.creationDate)
                .setInternal(new JobCreateParams.JobInternal(commandOptions.executionStatus))
                .setOutDir(commandOptions.outDir != null ? new JobCreateParams.TinyFile().setPath(commandOptions.outDir) : null)
                .setInput(commandOptions.input != null
                        ? commandOptions.input.stream().map(f -> new JobCreateParams.TinyFile().setPath(f)).collect(Collectors.toList())
                        : null)
                .setOutput(commandOptions.output != null
                        ? commandOptions.output.stream().map(f -> new JobCreateParams.TinyFile().setPath(f)).collect(Collectors.toList())
                        : null)
                .setTags(commandOptions.tags)
                .setTool(StringUtils.isNotEmpty(commandOptions.toolId) ? new ToolInfo(commandOptions.toolId, "", null, null, null) : null);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);

        return openCGAClient.getJobClient().create(createParams, params);
    }

    private RestResponse<Job> info() throws ClientException {
        logger.debug("Getting job information");

        JobCommandOptions.InfoCommandOptions commandOptions = jobsCommandOptions.infoCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotEmpty(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);

        return openCGAClient.getJobClient().info(commandOptions.job, params);
    }

    private RestResponse<Job> search() throws ClientException {
        logger.debug("Searching job");

        JobCommandOptions.SearchCommandOptions commandOptions = jobsCommandOptions.searchCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotEmpty(ParamConstants.JOB_ID_PARAM, commandOptions.id);
        params.putIfNotEmpty(ParamConstants.JOB_TOOL_ID_PARAM, commandOptions.toolId);
        params.putIfNotEmpty(ParamConstants.JOB_INTERNAL_STATUS_PARAM, commandOptions.internalStatus);
        params.putIfNotEmpty(ParamConstants.JOB_USER_PARAM, commandOptions.userId);
        params.putIfNotEmpty(ParamConstants.JOB_PRIORITY_PARAM, commandOptions.priority);
        params.putIfNotEmpty(ParamConstants.CREATION_DATE_PARAM, commandOptions.creationDate);
        params.putIfNotEmpty(ParamConstants.MODIFICATION_DATE_PARAM, commandOptions.modificationDate);
        params.putIfNotEmpty(ParamConstants.JOB_TAGS_PARAM, commandOptions.tags);
        params.putIfNotEmpty(ParamConstants.ACL_PARAM, commandOptions.acl);
        params.putIfNotEmpty(ParamConstants.JOB_INPUT_FILES_PARAM, commandOptions.inputFiles);
        params.putIfNotEmpty(ParamConstants.JOB_OUTPUT_FILES_PARAM, commandOptions.outputFiles);
        params.putAll(commandOptions.commonOptions.params);

        params.putIfNotNull(ParamConstants.RELEASE_PARAM, commandOptions.release);
        params.putIfNotNull(ParamConstants.OTHER_STUDIES_FLAG, commandOptions.otherStudies);
        params.putIfNotNull(ParamConstants.JOB_VISITED_PARAM, commandOptions.visited);
        params.putIfNotNull(ParamConstants.DELETED_PARAM, commandOptions.deleted);

        params.put(QueryOptions.COUNT, commandOptions.numericOptions.count);
        params.putIfNotEmpty(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);
        params.put(QueryOptions.LIMIT, commandOptions.numericOptions.limit);
        params.put(QueryOptions.SKIP, commandOptions.numericOptions.skip);

        return openCGAClient.getJobClient().search(params);
    }

    private void top() throws Exception {
        JobCommandOptions.TopCommandOptions c = jobsCommandOptions.topCommandOptions;

        Query query = new Query();
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), c.study);
        query.putIfNotEmpty(ParamConstants.JOB_TOOL_ID_PARAM, c.toolId);
        query.putIfNotEmpty(ParamConstants.JOB_INTERNAL_STATUS_PARAM, c.internalStatus);
        query.putIfNotEmpty(ParamConstants.JOB_USER_PARAM, c.userId);
        query.putIfNotEmpty(ParamConstants.JOB_PRIORITY_PARAM, c.priority);
        query.putAll(c.commonOptions.params);

        new JobsTopManager(openCGAClient, query, c.iterations, c.jobsLimit, c.delay, c.plain).run();
    }

    private void log() throws Exception {
        JobCommandOptions.LogCommandOptions c = jobsCommandOptions.logCommandOptions;
        new JobsLog(openCGAClient, c, System.out).run();
    }

    private RestResponse<Job> delete() throws ClientException {
        logger.debug("Deleting job");

        JobCommandOptions.DeleteCommandOptions commandOptions = jobsCommandOptions.deleteCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        return openCGAClient.getJobClient().delete(commandOptions.job, params);
    }

    private RestResponse<ObjectMap> updateAcl() throws ClientException, CatalogException {
        AclCommandOptions.AclsUpdateCommandOptions commandOptions = jobsCommandOptions.aclsUpdateCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);

        JobAclUpdateParams updateParams = new JobAclUpdateParams()
                .setJob(extractIdsFromListOrFile(commandOptions.id))
                .setPermissions(commandOptions.permissions);

        return openCGAClient.getJobClient().updateAcl(commandOptions.memberId, commandOptions.action.name(), updateParams);
    }

    private RestResponse<ObjectMap> acl() throws ClientException {
        AclCommandOptions.AclsCommandOptions commandOptions = jobsCommandOptions.aclsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
        params.putIfNotEmpty("member", commandOptions.memberId);

        params.putAll(commandOptions.commonOptions.params);

        return openCGAClient.getJobClient().acl(commandOptions.id, params);
    }
}
