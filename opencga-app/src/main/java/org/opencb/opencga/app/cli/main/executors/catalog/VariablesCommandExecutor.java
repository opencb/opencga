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


import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.catalog.VariableCommandOptions;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.VariableSet;

import java.io.File;
import java.io.IOException;

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
        QueryResponse queryResponse = null;
        switch (subCommandString) {
            case "create":
                queryResponse = create();
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
            case "field-add":
                queryResponse = fieldAdd();
                break;
            case "field-delete":
                queryResponse = fieldDelete();
                break;
            case "field-rename":
                queryResponse = fieldRename();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);

    }

    private QueryResponse<VariableSet> create() throws CatalogException, IOException {
        logger.debug("Creating variable");

        QueryOptions queryOptions = new QueryOptions();

        queryOptions.putIfNotNull("unique", variableCommandOptions.createCommandOptions.unique);
        queryOptions.putIfNotNull("description", variableCommandOptions.createCommandOptions.description);

        ObjectMapper mapper = new ObjectMapper();
        ObjectMap variables = mapper.readValue(new File(variableCommandOptions.createCommandOptions.jsonFile), ObjectMap.class);

        return openCGAClient.getVariableClient().create(variableCommandOptions.createCommandOptions.studyId,
                variableCommandOptions.createCommandOptions.name, variables.get("variables"), queryOptions);
    }

    private QueryResponse info() throws CatalogException {
        logger.debug("Getting variable information");
        return null;
    }

    private QueryResponse<VariableSet> search() throws CatalogException, IOException {
        logger.debug("Searching variable");
        Query query = new Query();
        query.put(SampleDBAdaptor.QueryParams.STUDY_ID.key(),variableCommandOptions.searchCommandOptions.studyId);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotNull("id", variableCommandOptions.searchCommandOptions.id);
        queryOptions.putIfNotNull("name", variableCommandOptions.searchCommandOptions.name);
        queryOptions.putIfNotNull("description", variableCommandOptions.searchCommandOptions.description);
        queryOptions.putIfNotNull("attributes", variableCommandOptions.searchCommandOptions.attributes);
        queryOptions.putIfNotNull(QueryOptions.INCLUDE, variableCommandOptions.searchCommandOptions.include);
        queryOptions.putIfNotNull(QueryOptions.EXCLUDE, variableCommandOptions.searchCommandOptions.exclude);
        queryOptions.putIfNotNull(QueryOptions.LIMIT, variableCommandOptions.searchCommandOptions.limit);
        queryOptions.putIfNotNull(QueryOptions.SKIP, variableCommandOptions.searchCommandOptions.skip);
        //TODO add when fixed the ws
        //queryOptions.putIfNotNull("count", variableCommandOptions.searchCommandOptions.count);

        return openCGAClient.getVariableClient().search(query, queryOptions);
    }

    private QueryResponse<VariableSet> update() throws CatalogException, IOException {
        logger.debug("Updating variable");
        ObjectMap objectMap = new ObjectMap();
        objectMap.putIfNotNull("name", variableCommandOptions.updateCommandOptions.name);
        objectMap.putIfNotNull("description", variableCommandOptions.updateCommandOptions.description);

        Object variables = null;
        if (variableCommandOptions.updateCommandOptions.jsonFile != null
                && !variableCommandOptions.updateCommandOptions.jsonFile.isEmpty()) {
            ObjectMapper mapper = new ObjectMapper();
            variables = mapper.readValue(new File(variableCommandOptions.updateCommandOptions.jsonFile), ObjectMap.class).get("variables");
        }

        return openCGAClient.getVariableClient().update(variableCommandOptions.updateCommandOptions.id, variables, objectMap);
    }

    private QueryResponse<VariableSet> delete() throws CatalogException, IOException {
        logger.debug("Deleting variable");
        ObjectMap objectMap = new ObjectMap();
        return openCGAClient.getVariableClient().delete(variableCommandOptions.deleteCommandOptions.id, objectMap);
    }

    private QueryResponse<VariableSet> fieldAdd() throws CatalogException, IOException {
        logger.debug("Adding variables to variable set");

        ObjectMapper mapper = new ObjectMapper();
        Object variables = mapper.readValue(new File(variableCommandOptions.fieldAddCommandOptions.jsonFile), ObjectMap.class)
                .get("variables");

        return openCGAClient.getVariableClient().addVariable(variableCommandOptions.fieldAddCommandOptions.id, variables);
    }

    private QueryResponse<VariableSet> fieldDelete() throws CatalogException, IOException {
        logger.debug("Deleting the variable field");
        ObjectMap objectMap = new ObjectMap();
        return openCGAClient.getVariableClient().fieldDelete(variableCommandOptions.fieldDeleteCommandOptions.id,
                variableCommandOptions.fieldDeleteCommandOptions.name, objectMap);
    }

    private QueryResponse<VariableSet> fieldRename() throws CatalogException, IOException {
        logger.debug("Rename the variable field");

        ObjectMap objectMap = new ObjectMap();
        return openCGAClient.getVariableClient().fieldRename(
                variableCommandOptions.fieldRenameCommandOptions.id, variableCommandOptions.fieldRenameCommandOptions.oldName,
                variableCommandOptions.fieldRenameCommandOptions.newName, objectMap);
    }


}
