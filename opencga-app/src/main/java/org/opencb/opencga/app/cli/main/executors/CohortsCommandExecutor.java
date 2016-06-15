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


import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.Sample;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class CohortsCommandExecutor extends OpencgaCommandExecutor {

    private OpencgaCliOptionsParser.CohortCommandsOptions cohortsCommandOptions;

    public CohortsCommandExecutor(OpencgaCliOptionsParser.CohortCommandsOptions cohortsCommandOptions) {
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
        String studyId = cohortsCommandOptions.createCommand.studyId;
        String cohortName = cohortsCommandOptions.createCommand.name;
        openCGAClient.getCohortClient().create(studyId,cohortName,null);
        logger.debug("Done");
    }
    private void info() throws CatalogException, IOException  {
        logger.debug("Getting cohort information");
        QueryResponse<Cohort> info = openCGAClient.getCohortClient().get(Long.toString(cohortsCommandOptions.infoCommand.id),null);
        System.out.println("Cohorts = " + info);
/************************************** Mirar si hay que blindar esto mas, converison de long a string, puede ser null? lo mismo en el de abajo******************////
    }
    private void samples() throws CatalogException, IOException  {
        logger.debug("Listing samples belonging to a cohort");
        QueryResponse<Sample> samples = openCGAClient.getCohortClient().getSamples(Long.toString(cohortsCommandOptions.samplesCommand.id), null);
        System.out.println("Samples = " + samples);
    }
    private void calculateStats() throws CatalogException {
        logger.debug("Calculating variant stats for a set of cohorts");
        //QueryResponse<Cohort> stats = openCGAClient.getCohortClient(). no esta calculate stats o parecidoo
        //TODO

    }




}
