/*
 * Copyright 2015 OpenCB
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


import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.options.catalog.JobCommandOptions;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.acls.permissions.JobAclEntry;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class JobsCommandExecutor extends OpencgaCommandExecutor {

    private JobCommandOptions jobsCommandOptions;
    private AclCommandExecutor<Job, JobAclEntry> aclCommandExecutor;

    public JobsCommandExecutor(JobCommandOptions jobsCommandOptions) {
        super(jobsCommandOptions.commonCommandOptions);
        this.jobsCommandOptions = jobsCommandOptions;
        this.aclCommandExecutor = new AclCommandExecutor<>();
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing jobs command line");

        String subCommandString = getParsedSubCommand(jobsCommandOptions.jCommander);
        QueryResponse queryResponse = null;
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
            case "acl-create":
                queryResponse = aclCommandExecutor.aclsCreate(jobsCommandOptions.aclsCreateCommandOptions, openCGAClient.getJobClient());
                break;
            case "acl-member-delete":
                queryResponse = aclCommandExecutor.aclMemberDelete(jobsCommandOptions.aclsMemberDeleteCommandOptions,
                        openCGAClient.getJobClient());
                break;
            case "acl-member-info":
                queryResponse = aclCommandExecutor.aclMemberInfo(jobsCommandOptions.aclsMemberInfoCommandOptions,
                        openCGAClient.getJobClient());
                break;
            case "acl-member-update":
                queryResponse = aclCommandExecutor.aclMemberUpdate(jobsCommandOptions.aclsMemberUpdateCommandOptions,
                        openCGAClient.getJobClient());
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    private QueryResponse<Job> create() throws CatalogException, IOException {
        logger.debug("Creating a new job");

        String studyId = jobsCommandOptions.createCommandOptions.studyId;
        String name = jobsCommandOptions.createCommandOptions.name;
        String toolId = jobsCommandOptions.createCommandOptions.toolId;
        String execution = jobsCommandOptions.createCommandOptions.execution;
        String description = jobsCommandOptions.createCommandOptions.description;

        ObjectMap o = new ObjectMap();
        o.append(JobDBAdaptor.QueryParams.EXECUTION.key(), execution);
        o.append(JobDBAdaptor.QueryParams.DESCRIPTION.key(), description);

        return openCGAClient.getJobClient().create(studyId, name, toolId, o);
    }

    private QueryResponse<Job> info() throws CatalogException, IOException {
        logger.debug("Getting job information");
        return openCGAClient.getJobClient().get(jobsCommandOptions.infoCommandOptions.id, null);
    }

    private QueryResponse<Job> search() throws CatalogException, IOException {
        logger.debug("Searching job");

        String studyId = jobsCommandOptions.searchCommandOptions.studyId;
        String name = jobsCommandOptions.searchCommandOptions.name;
        String toolName = jobsCommandOptions.searchCommandOptions.toolName;
        String status = jobsCommandOptions.searchCommandOptions.status;
        String ownerId = jobsCommandOptions.searchCommandOptions.ownerId;
        String date = jobsCommandOptions.searchCommandOptions.date;
        String input = jobsCommandOptions.searchCommandOptions.inputFiles;
        String output = jobsCommandOptions.searchCommandOptions.outputFiles;
        String include = jobsCommandOptions.searchCommandOptions.include;
        String exclude = jobsCommandOptions.searchCommandOptions.exclude;
        String limit = jobsCommandOptions.searchCommandOptions.limit;
        String skip = jobsCommandOptions.searchCommandOptions.skip;
        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions();

        query.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId );
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.NAME.key(), name);
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.TOOL_NAME.key(), toolName);
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.STATUS_NAME.key(), status);
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.USER_ID.key(), ownerId);
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.CREATION_DATE.key(), date);
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.INPUT.key(), input);
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.OUTPUT.key(), output);
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, exclude);
        queryOptions.putIfNotEmpty(QueryOptions.LIMIT, limit);
        queryOptions.putIfNotEmpty(QueryOptions.SKIP, skip);

        queryOptions.put("count", jobsCommandOptions.searchCommandOptions.count);
        return openCGAClient.getJobClient().search(query, queryOptions);
    }

    private QueryResponse<Job> visit() throws CatalogException, IOException {
        logger.debug("Visiting a job");
        QueryOptions queryOptions = new QueryOptions();
        return openCGAClient.getJobClient().visit(jobsCommandOptions.visitCommandOptions.id, queryOptions);
    }

    private QueryResponse<Job> delete() throws CatalogException, IOException {
        logger.debug("Deleting job");
        QueryOptions queryOptions = new QueryOptions();

        queryOptions.put("deleteFiles", jobsCommandOptions.deleteCommandOptions.deleteFiles);
        return openCGAClient.getJobClient().delete(jobsCommandOptions.deleteCommandOptions.id, queryOptions);
    }


    private QueryResponse<Job> groupBy() throws CatalogException, IOException {
        logger.debug("Group by job");

        ObjectMap objectMap = new ObjectMap();

        objectMap.putIfNotEmpty(JobDBAdaptor.QueryParams.ID.key(), jobsCommandOptions.groupByCommandOptions.id);
        objectMap.putIfNotEmpty(JobDBAdaptor.QueryParams.NAME.key(), jobsCommandOptions.groupByCommandOptions.name);
        objectMap.putIfNotEmpty(JobDBAdaptor.QueryParams.OUT_DIR_ID.key(), jobsCommandOptions.groupByCommandOptions.path);
        objectMap.putIfNotEmpty(JobDBAdaptor.QueryParams.STATUS_NAME.key(), jobsCommandOptions.groupByCommandOptions.status);
        objectMap.putIfNotEmpty(JobDBAdaptor.QueryParams.USER_ID.key(), jobsCommandOptions.groupByCommandOptions.ownerId);
        objectMap.putIfNotEmpty(JobDBAdaptor.QueryParams.CREATION_DATE.key(), jobsCommandOptions.groupByCommandOptions.creationDate);
        objectMap.putIfNotEmpty(JobDBAdaptor.QueryParams.DESCRIPTION.key(), jobsCommandOptions.groupByCommandOptions.description);
        objectMap.putIfNotEmpty(JobDBAdaptor.QueryParams.ATTRIBUTES.key(), jobsCommandOptions.groupByCommandOptions.attributes);

        return openCGAClient.getJobClient().groupBy(jobsCommandOptions.groupByCommandOptions.studyId,
                jobsCommandOptions.groupByCommandOptions.fields,objectMap);
    }

}
