/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.app.cli.main.executors.catalog;


import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.MetaCommandOptions;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.response.RestResponse;

/**
 * Created by agaor on 6/06/16.
 */
public class MetaCommandExecutor extends OpencgaCommandExecutor {

    private MetaCommandOptions metaCommandOptions;

    public MetaCommandExecutor(MetaCommandOptions metaCommandOptions) {
        super(metaCommandOptions.commonCommandOptions);
        this.metaCommandOptions = metaCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing individuals command line");

        String subCommandString = getParsedSubCommand(metaCommandOptions.jCommander);
        RestResponse queryResponse = null;
        switch (subCommandString) {
            case "status":
                queryResponse = status();
                break;
            case "about":
                queryResponse = about();
                break;
            case "ping":
                queryResponse = ping();
                break;
            case "api":
                queryResponse = api();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    @Override
    protected void loadCliSessionFile() {
        // Do not load session file for META commands
    }

    private RestResponse<?> status() throws ClientException {
        return openCGAClient.getMetaClient().status();
    }

    private RestResponse<?> api() throws ClientException {
        ObjectMap params = new ObjectMap(metaCommandOptions.commonCommandOptions.params);
        return openCGAClient.getMetaClient().api(params);
    }
    private RestResponse<?> about() throws ClientException {
        return openCGAClient.getMetaClient().about();
    }

    private RestResponse<?> ping() throws ClientException {
        return openCGAClient.getMetaClient().ping();
    }

}
