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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.ToolCommandOptions;
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
        switch (subCommandString) {
            case "help":
                help();
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

    private void help() throws CatalogException, IOException {
        logger.debug("Tool help");
        System.out.println("PENDING");

    }

    private void info() throws CatalogException, IOException {
        logger.debug("Getting tool information");
        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(toolsCommandOptions.infoCommandOptions.id)) {
            queryOptions.put("id", toolsCommandOptions.infoCommandOptions.id);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.infoCommandOptions.execution)) {
            queryOptions.put("execution", toolsCommandOptions.infoCommandOptions.execution);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.infoCommandOptions.commonOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, toolsCommandOptions.infoCommandOptions.commonOptions.include);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.infoCommandOptions.commonOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, toolsCommandOptions.infoCommandOptions.commonOptions.exclude);
        }
        QueryResponse<Tool> tools = openCGAClient.getToolClient().get(toolsCommandOptions.infoCommandOptions.id, queryOptions);
        tools.first().getResult().stream().forEach(tool -> System.out.println(tool.toString()));
    }

    private void search() throws CatalogException, IOException {
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

        if (StringUtils.isNotEmpty(toolsCommandOptions.searchCommandOptions.commonOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, toolsCommandOptions.searchCommandOptions.commonOptions.include);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.searchCommandOptions.commonOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, toolsCommandOptions.searchCommandOptions.commonOptions.exclude);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.searchCommandOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, toolsCommandOptions.searchCommandOptions.limit);
        }
        if (StringUtils.isNotEmpty(toolsCommandOptions.searchCommandOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, toolsCommandOptions.searchCommandOptions.skip);
        }
        queryOptions.put("count", toolsCommandOptions.searchCommandOptions.count);
        QueryResponse<Tool> tools = openCGAClient.getToolClient().get(toolsCommandOptions.searchCommandOptions.id, queryOptions);
        tools.first().getResult().stream().forEach(tool -> System.out.println(tool.toString()));
    }

    private void update() throws CatalogException, IOException {
        logger.debug("Updating tool");
        QueryOptions queryOptions = new QueryOptions();
        QueryResponse<Tool> tools = openCGAClient.getToolClient().update(toolsCommandOptions.updateCommandOptions.id, queryOptions);
        System.out.println(tools.toString());
    }

    private void delete() throws CatalogException, IOException {
        logger.debug("Deleting tool");
        QueryOptions queryOptions = new QueryOptions();
        QueryResponse<Tool> tools = openCGAClient.getToolClient().delete(toolsCommandOptions.deleteCommandOptions.id, queryOptions);
        System.out.println(tools.toString());
    }


}
