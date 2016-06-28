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
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.CohortCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Cohort;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class CohortsCommandExecutor extends OpencgaCommandExecutor {

    private CohortCommandOptions cohortsCommandOptions;

    public CohortsCommandExecutor(CohortCommandOptions cohortsCommandOptions) {
        super(cohortsCommandOptions.commonCommandOptions);
        this.cohortsCommandOptions = cohortsCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing cohorts command line");

        String subCommandString = getParsedSubCommand(cohortsCommandOptions.jCommander);
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
            case "annotate":
                annotate();
                break;
            case "update":
                update();
                break;
            case "delete":
                delete();
                break;
            case "unshare":
                unshare();
                break;
            case "calculate-stats":
                calculateStats();
                break;
            case "share":
                share();
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
        logger.debug("Creating a new cohort");
        String studyId = cohortsCommandOptions.createCommandOptions.studyId;
        String cohortName = cohortsCommandOptions.createCommandOptions.name;
        String description = cohortsCommandOptions.createCommandOptions.description;
        String variableSetId = cohortsCommandOptions.createCommandOptions.variableSetId;
        String sampleIds = cohortsCommandOptions.createCommandOptions.sampleIds;
        String variable = cohortsCommandOptions.createCommandOptions.variable;

        ObjectMap o = new ObjectMap();
        o.append(CatalogCohortDBAdaptor.QueryParams.TYPE.key(),description);
        o.append(CatalogCohortDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),variableSetId);
        o.append(CatalogCohortDBAdaptor.QueryParams.DESCRIPTION.key(),sampleIds);
        o.append(CatalogCohortDBAdaptor.QueryParams.SAMPLES.key(),sampleIds);
        o.append(CatalogCohortDBAdaptor.QueryParams.VARIABLE_NAME.key(),variable);
        openCGAClient.getCohortClient().create(studyId, cohortName, o);
        logger.debug("Done");
    }

    private void info() throws CatalogException, IOException {
        logger.debug("Getting cohort information");
        QueryResponse<Cohort> info =
                openCGAClient.getCohortClient().get(cohortsCommandOptions.infoCommandOptions.id, null);
        System.out.println("Cohorts = " + info);

    }

    private void samples() throws CatalogException, IOException {
        logger.debug("Listing samples belonging to a cohort");
        // QueryResponse<Sample> samples = openCGAClient.getCohortClient().getSamples(Long.toString(cohortsCommandOptions
        // .samplesCommandOptions.id), null);
        //System.out.println("Samples = " + samples);
    }

    private void annotate() throws CatalogException, IOException {
        logger.debug("Annotating cohort");
    }

    private void update() throws CatalogException, IOException {
        logger.debug("Updating cohort");

        ObjectMap objectMap = new ObjectMap();

        if (StringUtils.isNotEmpty(cohortsCommandOptions.updateCommandOptions.name)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.NAME.key(), cohortsCommandOptions.updateCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.updateCommandOptions.creationDate)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.CREATION_DATE.key(), cohortsCommandOptions.updateCommandOptions.creationDate);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.updateCommandOptions.description)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.DESCRIPTION.key(), cohortsCommandOptions.updateCommandOptions.description);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.updateCommandOptions.samples)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.SAMPLES.key(), cohortsCommandOptions.updateCommandOptions.samples);
        }


        QueryResponse<Cohort> cohort = openCGAClient.getCohortClient()
                .update(cohortsCommandOptions.updateCommandOptions.id, objectMap);
        System.out.println("Cohort: " + cohort);
    }

    private void delete() throws CatalogException, IOException {
        logger.debug("Deleting cohort");
        ObjectMap objectMap = new ObjectMap();
        QueryResponse<Cohort> cohort = openCGAClient.getCohortClient()
                .delete(cohortsCommandOptions.deleteCommandOptions.id, objectMap);
        System.out.println("Cohort: " + cohort);
    }

    private void unshare() throws CatalogException, IOException {
        logger.debug("Unsharing a cohort");
    }

    private void calculateStats() throws CatalogException {
        logger.debug("Calculating variant stats for a set of cohorts");
        //QueryResponse<Cohort> stats = openCGAClient.getCohortClient(). no esta calculate stats o parecidoo
        //TODO

    }

    private void share() throws CatalogException, IOException {
        logger.debug("Sharing a cohort");
    }

    private void groupBy() throws CatalogException, IOException {
        logger.debug("Group by cohorts");
    }

}
