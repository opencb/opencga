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


import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.JobCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;

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

    }
    private void info() throws CatalogException {
        logger.debug("Getting job information");
    }

    private void search() throws CatalogException, IOException  {
        logger.debug("Searching job");
    }

    private void visit() throws CatalogException, IOException  {
        logger.debug("Visiting a job");
    }

    private void delete() throws CatalogException, IOException  {
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
