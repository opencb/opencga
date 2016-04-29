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

package org.opencb.opencga.app.cli.analysis;


/**
 * Created by imedina on 02/03/15.
 */
public class VariantCommandExecutor extends CommandExecutor {

    private CliOptionsParser.VariantCommandOptions variantCommandOptions;

    public VariantCommandExecutor(CliOptionsParser.VariantCommandOptions variantCommandOptions) {
        super(variantCommandOptions.commonOptions);
        this.variantCommandOptions = variantCommandOptions;
    }



    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = variantCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "install":
                install();
                break;
            case "query":
                install();
                break;
            case "index":
                index();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void install() {
        // CatalogManager.getMetaManager.install();
        System.out.println(variantCommandOptions.indexVariantCommandOptions.password);
    }

    private void delete() {

    }

    private void index() {

    }

}
