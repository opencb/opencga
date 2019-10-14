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
import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.options.JobCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 03/06/16.
 */
public class JobCommandExecutor extends OpencgaCommandExecutor {

    private JobCommandOptions jobsCommandOptions;
    private AclCommandExecutor<Job> aclCommandExecutor;

    public JobCommandExecutor(JobCommandOptions jobsCommandOptions) {
        super(jobsCommandOptions.commonCommandOptions);
        this.jobsCommandOptions = jobsCommandOptions;
        this.aclCommandExecutor = new AclCommandExecutor<>();
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing jobs command line");

        String subCommandString = getParsedSubCommand(jobsCommandOptions.jCommander);
        DataResponse queryResponse = null;
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
            case "visit":
                queryResponse = visit();
                break;
            case "delete":
                queryResponse = delete();
                break;
            case "group-by":
                queryResponse = groupBy();
                break;
            case "acl":
                queryResponse = aclCommandExecutor.acls(jobsCommandOptions.aclsCommandOptions, openCGAClient.getJobClient());
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

    private DataResponse<Job> create() throws ClientException, IOException {
        logger.debug("Creating a new job");

        String studyId = resolveStudy(jobsCommandOptions.createCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.NAME.key(), jobsCommandOptions.createCommandOptions.name);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.TOOL_NAME.key(), jobsCommandOptions.createCommandOptions.toolName);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.DESCRIPTION.key(), jobsCommandOptions.createCommandOptions.description);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.EXECUTION.key(), jobsCommandOptions.createCommandOptions.execution);
        if (jobsCommandOptions.createCommandOptions.startTime > 0) {
            params.put(JobDBAdaptor.QueryParams.START_TIME.key(), jobsCommandOptions.createCommandOptions.startTime);
        }
        if (jobsCommandOptions.createCommandOptions.endTime > 0) {
            params.put(JobDBAdaptor.QueryParams.END_TIME.key(), jobsCommandOptions.createCommandOptions.endTime);
        }
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.COMMAND_LINE.key(), jobsCommandOptions.createCommandOptions.commandLine);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.OUT_DIR.key(), jobsCommandOptions.createCommandOptions.outDir);
        if (StringUtils.isNotEmpty(jobsCommandOptions.createCommandOptions.input)) {
            List<Long> list = new ArrayList<>();
            for (String aux : jobsCommandOptions.createCommandOptions.input.split(",")) {
                list.add(Long.parseLong(aux));
            }
            params.put(JobDBAdaptor.QueryParams.INPUT.key(), list);
        }
        if (StringUtils.isNotEmpty(jobsCommandOptions.createCommandOptions.output)) {
            List<Long> list = new ArrayList<>();
            for (String aux : jobsCommandOptions.createCommandOptions.output.split(",")) {
                list.add(Long.parseLong(aux));
            }
            params.put(JobDBAdaptor.QueryParams.OUTPUT.key(), list);
        }

        return openCGAClient.getJobClient().create(studyId, params);
    }

    private DataResponse<Job> info() throws CatalogException, IOException {
        logger.debug("Getting job information");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), resolveStudy(jobsCommandOptions.infoCommandOptions.study));
        params.putIfNotEmpty(QueryOptions.INCLUDE, jobsCommandOptions.infoCommandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, jobsCommandOptions.infoCommandOptions.dataModelOptions.exclude);
        return openCGAClient.getJobClient().get(jobsCommandOptions.infoCommandOptions.job, params);
    }

    private DataResponse<Job> search() throws CatalogException, IOException {
        logger.debug("Searching job");

        Query query = new Query();
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), resolveStudy(jobsCommandOptions.searchCommandOptions.study));
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.NAME.key(), jobsCommandOptions.searchCommandOptions.name);
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.TOOL_NAME.key(), jobsCommandOptions.searchCommandOptions.toolName);
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.STATUS_NAME.key(), jobsCommandOptions.searchCommandOptions.status);
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.USER_ID.key(), jobsCommandOptions.searchCommandOptions.ownerId);
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.CREATION_DATE.key(), jobsCommandOptions.searchCommandOptions.date);
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.INPUT.key(), jobsCommandOptions.searchCommandOptions.inputFiles);
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.OUTPUT.key(), jobsCommandOptions.searchCommandOptions.outputFiles);
        query.putAll(jobsCommandOptions.searchCommandOptions.commonOptions.params);

        if (jobsCommandOptions.searchCommandOptions.numericOptions.count) {
            return openCGAClient.getJobClient().count(query);
        } else {
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, jobsCommandOptions.searchCommandOptions.dataModelOptions.include);
            queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, jobsCommandOptions.searchCommandOptions.dataModelOptions.exclude);
            queryOptions.put(QueryOptions.LIMIT, jobsCommandOptions.searchCommandOptions.numericOptions.limit);
            queryOptions.put(QueryOptions.SKIP, jobsCommandOptions.searchCommandOptions.numericOptions.skip);

            return openCGAClient.getJobClient().search(query, queryOptions);
        }
    }

    private DataResponse<Job> visit() throws CatalogException, IOException {
        logger.debug("Visiting a job");
        Query query = new Query();
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), resolveStudy(jobsCommandOptions.visitCommandOptions.study));
        return openCGAClient.getJobClient().visit(jobsCommandOptions.visitCommandOptions.job, query);
    }

    private DataResponse<Job> delete() throws CatalogException, IOException {
        logger.debug("Deleting job");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), resolveStudy(jobsCommandOptions.deleteCommandOptions.study));
        params.put("deleteFiles", jobsCommandOptions.deleteCommandOptions.deleteFiles);
        return openCGAClient.getJobClient().delete(jobsCommandOptions.deleteCommandOptions.job, params);
    }

    private DataResponse<Job> groupBy() throws CatalogException, IOException {
        logger.debug("Group by job");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), resolveStudy(jobsCommandOptions.groupByCommandOptions.study));
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.UID.key(), jobsCommandOptions.groupByCommandOptions.id);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.NAME.key(), jobsCommandOptions.groupByCommandOptions.name);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.OUT_DIR.key(), jobsCommandOptions.groupByCommandOptions.path);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.STATUS_NAME.key(), jobsCommandOptions.groupByCommandOptions.status);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.USER_ID.key(), jobsCommandOptions.groupByCommandOptions.ownerId);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.CREATION_DATE.key(), jobsCommandOptions.groupByCommandOptions.creationDate);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.DESCRIPTION.key(), jobsCommandOptions.groupByCommandOptions.description);
        params.putIfNotEmpty(JobDBAdaptor.QueryParams.ATTRIBUTES.key(), jobsCommandOptions.groupByCommandOptions.attributes);

        return openCGAClient.getJobClient().groupBy(jobsCommandOptions.groupByCommandOptions.study,
                jobsCommandOptions.groupByCommandOptions.fields,params);
    }


    private DataResponse<ObjectMap> updateAcl() throws IOException, CatalogException {
        AclCommandOptions.AclsUpdateCommandOptions commandOptions = jobsCommandOptions.aclsUpdateCommandOptions;

        ObjectMap queryParams = new ObjectMap();

        ObjectMap bodyParams = new ObjectMap();
        bodyParams.putIfNotNull("permissions", commandOptions.permissions);
        bodyParams.putIfNotNull("action", commandOptions.action);
        bodyParams.putIfNotNull("job", extractIdsFromListOrFile(commandOptions.id));

        return openCGAClient.getJobClient().updateAcl(commandOptions.memberId, queryParams, bodyParams);
    }

}
