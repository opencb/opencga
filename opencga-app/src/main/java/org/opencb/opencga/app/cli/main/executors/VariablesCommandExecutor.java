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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.analysis.storage.variant.CatalogVariantDBAdaptor;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.VariableCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Variable;
import org.opencb.opencga.catalog.models.VariableSet;

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
        switch (subCommandString) {
            case "create":
                createOutput(create());
                break;
            case "info":
                createOutput(info());
                break;
            case "search":
                createOutput(search());
                break;
            case "update":
                createOutput(update());
                break;
            case "delete":
                createOutput(delete());
                break;
            case "field-delete":
                createOutput(fieldDelete());
                break;
            case "field-rename":
                createOutput(fieldRename());
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private QueryResponse<VariableSet> create() throws CatalogException, IOException {
        logger.debug("Creating variable");

        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(variableCommandOptions.createCommandOptions.studyId)) {
            queryOptions.put(CatalogProjectDBAdaptor.QueryParams.STUDY_ID.key(), variableCommandOptions.createCommandOptions.studyId);
        }
        if (StringUtils.isNotEmpty(variableCommandOptions.createCommandOptions.name)) {
            queryOptions.put(CatalogSampleDBAdaptor.QueryParams.NAME.key(), variableCommandOptions.createCommandOptions.name);
        }
        queryOptions.put("unique", variableCommandOptions.createCommandOptions.unique);
        if (StringUtils.isNotEmpty(variableCommandOptions.createCommandOptions.description)) {
            queryOptions.put(CatalogSampleDBAdaptor.QueryParams.DESCRIPTION.key(), variableCommandOptions.createCommandOptions.description);
        }
        return openCGAClient.getVariableClient().create(variableCommandOptions.createCommandOptions.studyId,
                variableCommandOptions.createCommandOptions.name,  queryOptions);
    }

    private QueryResponse info() throws CatalogException {
        logger.debug("Getting variable information");
        return null;
    }

    private QueryResponse<VariableSet> search() throws CatalogException, IOException {
        logger.debug("Searching variable");
        Query query = new Query();
        query.put(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(),variableCommandOptions.searchCommandOptions.studyId);

        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(variableCommandOptions.searchCommandOptions.id)) {
            queryOptions.put(CatalogSampleDBAdaptor.QueryParams.ID.key(), variableCommandOptions.searchCommandOptions.id);
        }

        if (StringUtils.isNotEmpty(variableCommandOptions.searchCommandOptions.name)) {
            queryOptions.put(CatalogSampleDBAdaptor.QueryParams.NAME.key(), variableCommandOptions.searchCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(variableCommandOptions.searchCommandOptions.description)) {
            queryOptions.put(CatalogSampleDBAdaptor.QueryParams.DESCRIPTION.key(), variableCommandOptions.searchCommandOptions.description);
        }
        if (StringUtils.isNotEmpty(variableCommandOptions.searchCommandOptions.attributes)) {
            queryOptions.put(CatalogSampleDBAdaptor.QueryParams.ATTRIBUTES.key(),
                    variableCommandOptions.searchCommandOptions.attributes);
        }

        if (StringUtils.isNotEmpty(variableCommandOptions.searchCommandOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, variableCommandOptions.searchCommandOptions.include);
        }
        if (StringUtils.isNotEmpty(variableCommandOptions.searchCommandOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, variableCommandOptions.searchCommandOptions.exclude);
        }
        if (StringUtils.isNotEmpty(variableCommandOptions.searchCommandOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, variableCommandOptions.searchCommandOptions.limit);
        }
        if (StringUtils.isNotEmpty(variableCommandOptions.searchCommandOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, variableCommandOptions.searchCommandOptions.skip);
        }

        queryOptions.put("count", variableCommandOptions.searchCommandOptions.count);

        return openCGAClient.getVariableClient().search(query, queryOptions);
    }

    private QueryResponse<VariableSet> update() throws CatalogException, IOException {
        logger.debug("Updating variable");
        ObjectMap objectMap = new ObjectMap();
        if (StringUtils.isNotEmpty(variableCommandOptions.updateCommandOptions.name)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.NAME.key(), variableCommandOptions.updateCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(variableCommandOptions.updateCommandOptions.description)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.DESCRIPTION.key(), variableCommandOptions.updateCommandOptions.description);
        }
        return openCGAClient.getVariableClient().update(variableCommandOptions.updateCommandOptions.id, objectMap);
    }

    private QueryResponse<VariableSet> delete() throws CatalogException, IOException {
        logger.debug("Deleting variable");
        ObjectMap objectMap = new ObjectMap();
        return openCGAClient.getVariableClient().delete(variableCommandOptions.deleteCommandOptions.id, objectMap);
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
