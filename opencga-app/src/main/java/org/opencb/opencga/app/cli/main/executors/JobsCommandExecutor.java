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
import org.opencb.opencga.catalog.exceptions.CatalogException;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class JobsCommandExecutor extends OpencgaCommandExecutor {

    private OpencgaCliOptionsParser.JobsCommandsOptions jobsCommandOptions;

    public JobsCommandExecutor(OpencgaCliOptionsParser.JobsCommandsOptions jobsCommandOptions) {
        super(jobsCommandOptions.commonOptions);
        this.jobsCommandOptions = jobsCommandOptions;
    }



    @Override
    public void execute() throws Exception {
        logger.debug("Executing jobs command line");

        String subCommandString = jobsCommandOptions.getParsedSubCommand();
        switch (subCommandString) {

            case "info":
                info();
                break;
            case "finished":
                finished();
                break;
            case "status":
                status();
                break;
            case "run":
                run();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void info() throws CatalogException {
        logger.debug("Getting job information");
    }
    private void finished() throws CatalogException, IOException {
        logger.debug("Notice catalog that a job have finished");
    }
    private void status() throws CatalogException {
        logger.debug("Getting the status of all running jobs");
    }
    private void run() throws CatalogException {
        logger.debug("Executing a job");
    }




}
