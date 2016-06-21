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


import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.SampleCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class SamplesCommandExecutor extends OpencgaCommandExecutor {

    private SampleCommandOptions samplesCommandOptions;

    public SamplesCommandExecutor(SampleCommandOptions samplesCommandOptions) {
        super(samplesCommandOptions.commonCommandOptions);
        this.samplesCommandOptions = samplesCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing samples command line");

        String subCommandString = getParsedSubCommand(samplesCommandOptions.jCommander);
        switch (subCommandString) {
            case "create":
                create();
                break;
            case "load":
                load();
                break;
            case "info":
                info();
                break;
            case "search":
                search();
                break;
            case "update":
                update();
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
            case "groupBy":
                groupBy();
                break;
            case "annotate":
                annotate();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void create() throws CatalogException, IOException {
        logger.debug("Creating sample");
    }

    private void load() throws CatalogException, IOException {
        logger.debug("Loading samples from a pedigree file");
    }

    private void info() throws CatalogException {
        logger.debug("Getting samples information");
    }

    private void search() throws CatalogException {
        logger.debug("Searching samples");
    }

    private void update() throws CatalogException {
        logger.debug("Updating samples");
    }

    private void delete() throws CatalogException {
        logger.debug("Deleting the select sample");
    }

    private void share() throws CatalogException {
        logger.debug("Sharing samples");
    }

    private void unshare() throws CatalogException {
        logger.debug("Unsharing samples");
    }

    private void groupBy() throws CatalogException {
        logger.debug("Group By samples");
    }

    private void annotate() throws CatalogException {
        logger.debug("Annotating samples");
    }


}
