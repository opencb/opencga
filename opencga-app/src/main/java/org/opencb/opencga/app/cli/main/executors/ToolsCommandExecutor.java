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
 * Created by imedina on 03/06/16.
 */
public class ToolsCommandExecutor extends OpencgaCommandExecutor {

    private OpencgaCliOptionsParser.ToolCommandsOptions toolsCommandOptions;

    public ToolsCommandExecutor(OpencgaCliOptionsParser.ToolCommandsOptions toolsCommandOptions) {
        super(toolsCommandOptions.commonOptions);
        this.toolsCommandOptions = toolsCommandOptions;
    }



    @Override
    public void execute() throws Exception {
        logger.debug("Executing tools command line");

        String subCommandString = toolsCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "create":
                create();
                break;
            case "info":
                info();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void create() throws CatalogException {
        logger.debug("Recording external tool into catalog");
    }
    private void info() throws CatalogException {
        logger.debug("Getting tool information");
    }






}
