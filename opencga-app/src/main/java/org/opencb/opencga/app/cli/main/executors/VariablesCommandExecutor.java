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
import org.opencb.opencga.app.cli.main.options.ToolCommandOptions;
import org.opencb.opencga.app.cli.main.options.VariableCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * Created by by sgallego on 6/15/16.
 */
public class VariablesCommandExecutor extends OpencgaCommandExecutor {

    private VariableCommandOptions variableCommandOptions;

    public VariablesCommandExecutor(VariableCommandOptions variableCommandOptions) {
        super(variableCommandOptions.commonCommandOptions);
        this.variableCommandOptions = variableCommandOptions;
    }



    @Override
    public void execute() throws Exception {
        logger.debug("Executing variables command line");

        String subCommandString = getParsedSubCommand(variableCommandOptions.jCommander);
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
        logger.debug("Creating variable");
    }
    private void info() throws CatalogException {
        logger.debug("Getting variable information");
    }

    private void search() throws CatalogException {
        logger.debug("Searching variable");
    }
    private void update() throws CatalogException {
        logger.debug("Updating variable");
    }
    private void delete() throws CatalogException {
        logger.debug("Deleting variable");
    }






}
