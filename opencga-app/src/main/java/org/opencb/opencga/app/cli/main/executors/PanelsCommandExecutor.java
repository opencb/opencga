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
import org.opencb.opencga.app.cli.main.options.PanelCommandOptions;
import org.opencb.opencga.app.cli.main.options.ToolCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * Created by sgallego on 6/15/16.
 */
public class PanelsCommandExecutor extends OpencgaCommandExecutor {

    private PanelCommandOptions panelsCommandOptions;

    public PanelsCommandExecutor(PanelCommandOptions panelsCommandOptions) {
        super(panelsCommandOptions.commonCommandOptions);
        this.panelsCommandOptions = panelsCommandOptions;
    }



    @Override
    public void execute() throws Exception {
        logger.debug("Executing panels command line");

        String subCommandString = getParsedSubCommand(panelsCommandOptions.jCommander);
        switch (subCommandString) {
            case "create":
                create();
                break;
            case "info":
                info();
                break;
            case "share":
                share();
                break;
            case "unshare":
                unshare();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void create() throws CatalogException {
        logger.debug("Creating a panel");
    }
    private void info() throws CatalogException {
        logger.debug("Getting panel information");
    }

    private void share() throws CatalogException {
        logger.debug("Sharing panel");
    }

    private void unshare() throws CatalogException {
        logger.debug("Unsharing panel");
    }




}
