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

package org.opencb.opencga.app.cli.main.executors;


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.JobCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class JobsCommandExecutor extends OpencgaCommandExecutor {

    private JobCommandOptions jobsCommandOptions;

    public JobsCommandExecutor(JobCommandOptions jobsCommandOptions) {
        super(jobsCommandOptions.commonCommandOptions);
        this.jobsCommandOptions = jobsCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing jobs command line");

        String subCommandString = getParsedSubCommand(jobsCommandOptions.jCommander);
        switch (subCommandString) {

            case "create":
                create();
                break;
            case "info":
                info();
                break;
            case "search":
                search();
                break;
            case "visit":
                visit();
                break;
            case "delete":
                delete();
                break;
            case "share":
                share();
                break;
            case "unshare":
                unshare();
                break;
            case "group-by":
                groupBy();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void create() throws CatalogException, IOException {
        logger.debug("Creating a new job");

        String studyId = jobsCommandOptions.createCommandOptions.studyId;
        String name = jobsCommandOptions.createCommandOptions.name;
        String toolId = jobsCommandOptions.createCommandOptions.toolId;
        String execution = jobsCommandOptions.createCommandOptions.execution;
        String description = jobsCommandOptions.createCommandOptions.description;

        ObjectMap o = new ObjectMap();
        o.append(CatalogJobDBAdaptor.QueryParams.EXECUTION.key(),execution);
        o.append(CatalogJobDBAdaptor.QueryParams.DESCRIPTION.key(),description);

        openCGAClient.getJobClient().create(studyId, name, toolId, o);
        logger.debug("Done");

    }

    private void info() throws CatalogException, IOException {
        logger.debug("Getting job information");
        QueryResponse<Job> info =
                openCGAClient.getJobClient().get(Integer.toString(jobsCommandOptions.infoCommandOptions.id), null);
        System.out.println("Cohorts = " + info);
    }

    private void search() throws CatalogException, IOException {
        logger.debug("Searching job");

        String studyId = jobsCommandOptions.searchCommandOptions.studyId;
        String name = jobsCommandOptions.searchCommandOptions.name;
        String path = jobsCommandOptions.searchCommandOptions.path;
        String status = jobsCommandOptions.searchCommandOptions.status;
        String ownerId = jobsCommandOptions.searchCommandOptions.ownerId;
        String creationDate = jobsCommandOptions.searchCommandOptions.creationDate;
        String modificationDate = jobsCommandOptions.searchCommandOptions.modificationDate;
        String description = jobsCommandOptions.searchCommandOptions.description;
        String jobId = jobsCommandOptions.searchCommandOptions.jobId;
        String attributes = jobsCommandOptions.searchCommandOptions.attributes;
        String nattributes = jobsCommandOptions.searchCommandOptions.nattributes;

        Query query = new Query();

      /*  if (StringUtils.isNotEmpty(studyId)) {
            query.put(CatalogJobDBAdaptor.QueryParams.STUDY_ID.key(), studyId );
        }

        if (StringUtils.isNotEmpty(name)) {
            query.put(CatalogJobDBAdaptor.QueryParams.NAME.key(), name);
        }

        if (StringUtils.isNotEmpty(path)) {
            query.put(CatalogJobDBAdaptor.QueryParams.OUT_DIR_ID.key(), path);
        }

        if (StringUtils.isNotEmpty(status)) {
            query.put(CatalogJobDBAdaptor.QueryParams.STATUS_STATUS.key(), status);
        }

        if (StringUtils.isNotEmpty(ownerId)) {
            query.put(CatalogJobDBAdaptor.QueryParams. .key(), type);
        }

        if (StringUtils.isNotEmpty(creationDate)) {
            query.put(CatalogJobDBAdaptor.QueryParams.CREATION_DATE.key(), creationDate);
        }

        if (StringUtils.isNotEmpty(status)) {
            query.put(CatalogJobDBAdaptor.QueryParams.STATUS_STATUS.key(), status);
        }

        if (StringUtils.isNotEmpty(attributes)) {
            query.put(CatalogJobDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);
        }

        if (StringUtils.isNotEmpty(nattributes)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.NATTRIBUTES.key(), nattributes);
        }

        if (battributes) {
            query.put(CatalogStudyDBAdaptor.QueryParams.BATTRIBUTES.key(), battributes);
        }

        if (StringUtils.isNotEmpty(groups)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.GROUPS.key(), groups);
        }

        if (StringUtils.isNotEmpty(groupsUsers)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.GROUP_USER_IDS.key(), groupsUsers);
        }

        QueryResponse<Study> studies = openCGAClient.getStudyClient().search(query, null);
        System.out.println("Studies: " + studies);*/


    }

    private void visit() throws CatalogException, IOException {
        logger.debug("Visiting a job");
    }

    private void delete() throws CatalogException, IOException {
        logger.debug("Deleting job");
    }

    private void unshare() throws CatalogException, IOException {
        logger.debug("Unsharing a job");
    }

    private void share() throws CatalogException, IOException {
        logger.debug("Sharing a job");
    }

    private void groupBy() throws CatalogException, IOException {
        logger.debug("Group by job");
    }


}
