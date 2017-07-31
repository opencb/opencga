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


import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.ToolCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Tool;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class ToolCommandExecutor extends OpencgaCommandExecutor {

    private ToolCommandOptions toolsCommandOptions;

    public ToolCommandExecutor(ToolCommandOptions toolsCommandOptions) {
        super(toolsCommandOptions.commonCommandOptions);
        this.toolsCommandOptions = toolsCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing tools command line");

        String subCommandString = getParsedSubCommand(toolsCommandOptions.jCommander);
        QueryResponse queryResponse = null;
        switch (subCommandString) {
            case "help":
                queryResponse = help();
                break;
            case "info":
                queryResponse = info();
                break;
            case "search":
                queryResponse = search();
                break;
            case "update":
                queryResponse = update();
                break;
            case "delete":
                queryResponse = delete();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    private QueryResponse help() throws CatalogException, IOException {
        logger.debug("Tool help");
        System.out.println("PENDING");
        return null;
    }

    private QueryResponse<Tool> info() throws CatalogException, IOException {
        logger.debug("Getting tool information");

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty("id", toolsCommandOptions.infoCommandOptions.id);
        queryOptions.putIfNotEmpty("execution", toolsCommandOptions.infoCommandOptions.execution);
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, toolsCommandOptions.infoCommandOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, toolsCommandOptions.infoCommandOptions.exclude);
        return openCGAClient.getToolClient().get(toolsCommandOptions.infoCommandOptions.id, queryOptions);
    }

    private QueryResponse<Tool> search() throws CatalogException, IOException {
        logger.debug("Searching tool");

        Query query = new Query();
        query.putIfNotEmpty("id", toolsCommandOptions.searchCommandOptions.id);
        query.putIfNotEmpty("userId", toolsCommandOptions.searchCommandOptions.userId);
        query.putIfNotEmpty("alias", toolsCommandOptions.searchCommandOptions.alias);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, toolsCommandOptions.searchCommandOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, toolsCommandOptions.searchCommandOptions.exclude);
        queryOptions.putIfNotEmpty(QueryOptions.LIMIT, toolsCommandOptions.searchCommandOptions.limit);
        queryOptions.putIfNotEmpty(QueryOptions.SKIP, toolsCommandOptions.searchCommandOptions.skip);
        queryOptions.put("count", toolsCommandOptions.searchCommandOptions.count);
        return openCGAClient.getToolClient().search(query, queryOptions);
    }

    private QueryResponse<Tool> update() throws CatalogException, IOException {
        logger.debug("Updating tool");

        return openCGAClient.getToolClient().update(toolsCommandOptions.updateCommandOptions.id,
                null, QueryOptions.empty());
    }

    private QueryResponse<Tool> delete() throws CatalogException, IOException {
        logger.debug("Deleting tool");

        return openCGAClient.getToolClient().delete(toolsCommandOptions.deleteCommandOptions.id, QueryOptions.empty());
    }


}
