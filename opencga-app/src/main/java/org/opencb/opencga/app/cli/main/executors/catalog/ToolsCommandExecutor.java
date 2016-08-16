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

package org.opencb.opencga.app.cli.main.executors.catalog;


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.catalog.ToolCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Tool;


import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class ToolsCommandExecutor extends OpencgaCommandExecutor {

    private ToolCommandOptions toolsCommandOptions;

    public ToolsCommandExecutor(ToolCommandOptions toolsCommandOptions) {
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
        if (StringUtils.isNotEmpty(toolsCommandOptions.infoCommandOptions.id)) {
            queryOptions.put("id", toolsCommandOptions.infoCommandOptions.id);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.infoCommandOptions.execution)) {
            queryOptions.put("execution", toolsCommandOptions.infoCommandOptions.execution);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.infoCommandOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, toolsCommandOptions.infoCommandOptions.include);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.infoCommandOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, toolsCommandOptions.infoCommandOptions.exclude);
        }
        return openCGAClient.getToolClient().get(toolsCommandOptions.infoCommandOptions.id, queryOptions);
    }

    private QueryResponse<Tool> search() throws CatalogException, IOException {
        logger.debug("Searching tool");
        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(toolsCommandOptions.searchCommandOptions.id)) {
            queryOptions.put("id", toolsCommandOptions.searchCommandOptions.id);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.searchCommandOptions.userId)) {
            queryOptions.put("userId", toolsCommandOptions.searchCommandOptions.userId);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.searchCommandOptions.alias)) {
            queryOptions.put("alias", toolsCommandOptions.searchCommandOptions.alias);
        }

        if (StringUtils.isNotEmpty(toolsCommandOptions.searchCommandOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, toolsCommandOptions.searchCommandOptions.include);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.searchCommandOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, toolsCommandOptions.searchCommandOptions.exclude);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.searchCommandOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, toolsCommandOptions.searchCommandOptions.limit);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.searchCommandOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, toolsCommandOptions.searchCommandOptions.skip);
        }
        queryOptions.put("count", toolsCommandOptions.searchCommandOptions.count);
        return openCGAClient.getToolClient().get(toolsCommandOptions.searchCommandOptions.id, queryOptions);
    }

    private QueryResponse<Tool> update() throws CatalogException, IOException {
        logger.debug("Updating tool");
        QueryOptions queryOptions = new QueryOptions();
        return openCGAClient.getToolClient().update(toolsCommandOptions.updateCommandOptions.id, queryOptions);
    }

    private QueryResponse<Tool> delete() throws CatalogException, IOException {
        logger.debug("Deleting tool");
        QueryOptions queryOptions = new QueryOptions();
        return openCGAClient.getToolClient().delete(toolsCommandOptions.deleteCommandOptions.id, queryOptions);
    }


}
