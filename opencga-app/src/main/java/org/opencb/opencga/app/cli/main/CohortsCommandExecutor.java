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

package org.opencb.opencga.app.cli.main;


import org.opencb.opencga.catalog.exceptions.CatalogException;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class CohortsCommandExecutor extends OpencgaCommandExecutor {

    private OpencgaCliOptionsParser.UsersCommandOptions cohortsCommandOptions;

    public CohortsCommandExecutor(OpencgaCliOptionsParser.UsersCommandOptions cohortsCommandOptions) {
        super(cohortsCommandOptions.commonOptions);
        this.cohortsCommandOptions = cohortsCommandOptions;
    }



    @Override
    public void execute() throws Exception {
        logger.debug("Executing cohorts command line");

        String subCommandString = cohortsCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "create":
                create();
                break;

            case "info":
                info();
                break;
            case "samples":
                samples();
                break;
            case "calculate-stats":
                calculateStats();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void create() throws CatalogException, IOException {
        logger.debug("Creating a new cohort");
    }
    private void info() throws CatalogException {
        logger.debug("Getting cohort information");
    }
    private void samples() throws CatalogException {
        logger.debug("Listing samples belonging to a cohort");
    }
    private void calculateStats() throws CatalogException {
        logger.debug("Calculating variant stats for a set of cohorts");
    }




}
