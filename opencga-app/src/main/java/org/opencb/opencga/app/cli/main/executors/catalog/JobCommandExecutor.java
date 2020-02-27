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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.JobCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.FileContent;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobAclUpdateParams;
import org.opencb.opencga.core.models.job.JobCreateParams;
import org.opencb.opencga.core.models.job.ToolInfo;
import org.opencb.opencga.core.response.RestResponse;

import java.util.concurrent.TimeUnit;
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
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), resolveStudy(commandOptions.study));

        return openCGAClient.getJobClient().create(createParams, params);
    }

    private RestResponse<Job> info() throws ClientException {
        logger.debug("Getting job information");

        JobCommandOptions.InfoCommandOptions commandOptions = jobsCommandOptions.infoCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), resolveStudy(commandOptions.study));
        params.putIfNotEmpty(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);

        return openCGAClient.getJobClient().info(commandOptions.job, params);
    }

    private RestResponse<Job> search() throws ClientException {
        logger.debug("Searching job");

        JobCommandOptions.SearchCommandOptions commandOptions = jobsCommandOptions.searchCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), resolveStudy(commandOptions.study));
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.ID.key(), commandOptions.id);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.TOOL_NAME.key(), commandOptions.toolName);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), commandOptions.status);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.USER_ID.key(), commandOptions.ownerId);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.CREATION_DATE.key(), commandOptions.date);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.INPUT.key(), commandOptions.inputFiles);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.OUTPUT.key(), commandOptions.outputFiles);
        params.putAll(commandOptions.commonOptions.params);

        params.put(QueryOptions.COUNT, commandOptions.numericOptions.count);
        params.putIfNotEmpty(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);
        params.put(QueryOptions.LIMIT, commandOptions.numericOptions.limit);
        params.put(QueryOptions.SKIP, commandOptions.numericOptions.skip);

        return openCGAClient.getJobClient().search(params);
    }

    private void top() throws Exception {
        JobCommandOptions.TopCommandOptions c = jobsCommandOptions.topCommandOptions;
        String study = resolveStudy(c.study);
        new JobsTop(openCGAClient, study, c.iterations, c.jobsLimit, c.delay).run();
    }

    private void log() throws Exception {
        JobCommandOptions.LogCommandOptions c = jobsCommandOptions.logCommandOptions;
        String study = resolveStudy(c.study);
        openCGAClient.setThrowExceptionOnError(true);

        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study).append("type", c.type);

        int lines;
        if (c.lines == null) {
            lines = Integer.MAX_VALUE;
            params.append("tail", false).append("lines", 100);
        } else {
            if (c.follow) {
                // Force tail = true
                c.tail = true;
            }
            lines = c.lines;
            params.append("tail", c.tail).append("lines", lines);
        }

        int printedLines = 0;
        FileContent content = openCGAClient.getJobClient().log(c.job, params).firstResult();
        printedLines += printContent(content);

        params.put("lines", 100);
        params.put("tail", false); // Only use tail for the first batch

        while (c.follow || printedLines < lines ) {
            params.put("offset", content.getOffset());
            content = openCGAClient.getJobClient().log(c.job, params).firstResult();
            printedLines += printContent(content);

            // Read fewer lines than expected
            if (content.getLines() < params.getInt("lines")) {
                if (c.follow) {
                    // Check job status
                    Job job = openCGAClient.getJobClient().info(c.job, new ObjectMap(ParamConstants.STUDY_PARAM, study)).firstResult();
                    if (job.getInternal().getStatus().getName().equals(Enums.ExecutionStatus.RUNNING)) {
                        // If the job is still running, sleep and continue
                        Thread.sleep(TimeUnit.SECONDS.toMillis(c.delay));
                    } else {
                        // If the job is not running, skip sleep and break loop
                        break;
                    }
                } else {
                    // End of file
                    if (content.isEof()) {
                        break;
                    }
                }
            }
        }
    }

    private int printContent(FileContent content) {
        if (!content.getContent().isEmpty()) {
            System.out.print(content.getContent());
            if (!content.getContent().endsWith("\n")) {
                System.out.println();
            }
            System.out.flush();
        }
        return content.getLines();
    }

    private RestResponse<Job> delete() throws ClientException {
        logger.debug("Deleting job");

        JobCommandOptions.DeleteCommandOptions commandOptions = jobsCommandOptions.deleteCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), resolveStudy(commandOptions.study));
        params.put("deleteFiles", commandOptions.deleteFiles);
        return openCGAClient.getJobClient().delete(commandOptions.job, params);
    }

    private RestResponse<ObjectMap> updateAcl() throws ClientException, CatalogException {
        AclCommandOptions.AclsUpdateCommandOptions commandOptions = jobsCommandOptions.aclsUpdateCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);

        JobAclUpdateParams updateParams = new JobAclUpdateParams()
                .setJob(extractIdsFromListOrFile(commandOptions.id))
                .setPermissions(commandOptions.permissions)
                .setAction(commandOptions.action);

        return openCGAClient.getJobClient().updateAcl(commandOptions.memberId, updateParams);
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
