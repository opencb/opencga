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

/**
 * Created by agaor on 6/06/16.
 */
public class IndividualsCommandExecutor extends OpencgaCommandExecutor {

    private OpencgaCliOptionsParser.IndividualsCommandsOptions individualsCommandOptions;

    public IndividualsCommandExecutor(OpencgaCliOptionsParser.IndividualsCommandsOptions jobsCommandOptions) {
        super(jobsCommandOptions.commonOptions);
        this.individualsCommandOptions = jobsCommandOptions;
    }



    @Override
    public void execute() throws Exception {
        logger.debug("Executing jobs command line");

        String subCommandString = individualsCommandOptions.getParsedSubCommand();
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
            case "annotate":
                annotate();
                break;
            case "update":
                update();
                break;
            case "delete":
                delete();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void create() throws CatalogException {
        logger.debug("Creating sample");
    }


    private void info() throws CatalogException {
        logger.debug("Getting individual information");
    }

    private void search() throws CatalogException {
        logger.debug("Searching individuals");
    }
    private void annotate() throws CatalogException {
        logger.debug("Annotating an individual");
    }
    private void update() throws CatalogException {
        logger.debug("Updating individual information");
    }
    private void delete() throws CatalogException {
        logger.debug("Deleting individual information");
    }



}
