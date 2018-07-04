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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.options.PanelCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.catalog.db.api.DiseasePanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.DiseasePanel;
import org.opencb.opencga.core.models.acls.permissions.DiseasePanelAclEntry;

import java.io.IOException;

/**
 * Created by sgallego on 6/15/16.
 */
public class PanelCommandExecutor extends OpencgaCommandExecutor {

    private PanelCommandOptions panelsCommandOptions;
    private AclCommandExecutor<DiseasePanel, DiseasePanelAclEntry> aclCommandExecutor;

    public PanelCommandExecutor(PanelCommandOptions panelsCommandOptions) {
        super(panelsCommandOptions.commonCommandOptions);
        this.panelsCommandOptions = panelsCommandOptions;
        this.aclCommandExecutor = new AclCommandExecutor<>();
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing panels command line");

        String subCommandString = getParsedSubCommand(panelsCommandOptions.jCommander);
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
            case "acl":
                queryResponse = aclCommandExecutor.acls(panelsCommandOptions.aclsCommandOptions, openCGAClient.getPanelClient());
                break;
            case "acl-update":
                queryResponse = updateAcl();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);

    }

    /**********************************************  Administration Commands  ***********************************************/
    private QueryResponse<DiseasePanel> create() throws CatalogException, IOException {
        logger.debug("Creating a new panel");
        String name = panelsCommandOptions.createCommandOptions.name;
        String disease = panelsCommandOptions.createCommandOptions.disease;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(DiseasePanelDBAdaptor.QueryParams.DESCRIPTION.key(), panelsCommandOptions.createCommandOptions.description);
        params.putIfNotEmpty(DiseasePanelDBAdaptor.QueryParams.GENES.key(), panelsCommandOptions.createCommandOptions.genes);
        params.putIfNotEmpty(DiseasePanelDBAdaptor.QueryParams.REGIONS.key(), panelsCommandOptions.createCommandOptions.regions);
        params.putIfNotEmpty(DiseasePanelDBAdaptor.QueryParams.VARIANTS.key(), panelsCommandOptions.createCommandOptions.variants);
        return openCGAClient.getPanelClient().create(resolveStudy(panelsCommandOptions.createCommandOptions.studyId), name, disease, params);
    }

    private QueryResponse<DiseasePanel> info() throws CatalogException, IOException  {
        logger.debug("Getting panel information");

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("study", resolveStudy(panelsCommandOptions.infoCommandOptions.study));
        params.putIfNotEmpty(QueryOptions.INCLUDE, panelsCommandOptions.infoCommandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, panelsCommandOptions.infoCommandOptions.dataModelOptions.exclude);
        return openCGAClient.getPanelClient().get(panelsCommandOptions.infoCommandOptions.id, params);
    }

    private QueryResponse<DiseasePanel> search() throws CatalogException, IOException  {
        logger.debug("Searching panels");

        Query query = new Query();
        query.putIfNotNull("study", resolveStudy(panelsCommandOptions.searchCommandOptions.study));
        query.putIfNotNull(DiseasePanelDBAdaptor.QueryParams.NAME.key(), panelsCommandOptions.searchCommandOptions.name);
        query.putIfNotNull(DiseasePanelDBAdaptor.QueryParams.PHENOTYPES.key(), panelsCommandOptions.searchCommandOptions.phenotypes);
        query.putIfNotNull(DiseasePanelDBAdaptor.QueryParams.VARIANTS.key(), panelsCommandOptions.searchCommandOptions.variants);
        query.putIfNotNull(DiseasePanelDBAdaptor.QueryParams.REGIONS.key(), panelsCommandOptions.searchCommandOptions.regions);
        query.putIfNotNull(DiseasePanelDBAdaptor.QueryParams.GENES.key(), panelsCommandOptions.searchCommandOptions.genes);
        query.putIfNotNull(DiseasePanelDBAdaptor.QueryParams.DESCRIPTION.key(), panelsCommandOptions.searchCommandOptions.description);
        query.putIfNotNull(DiseasePanelDBAdaptor.QueryParams.AUTHOR.key(), panelsCommandOptions.searchCommandOptions.author);
        query.putIfNotNull(DiseasePanelDBAdaptor.QueryParams.CREATION_DATE.key(), panelsCommandOptions.searchCommandOptions.creationDate);
        query.putIfNotNull(DiseasePanelDBAdaptor.QueryParams.RELEASE.key(), panelsCommandOptions.searchCommandOptions.release);
        query.putIfNotNull(DiseasePanelDBAdaptor.QueryParams.SNAPSHOT.key(), panelsCommandOptions.searchCommandOptions.snapshot);

        if (panelsCommandOptions.searchCommandOptions.numericOptions.count) {
            return openCGAClient.getPanelClient().count(query);
        } else {
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, panelsCommandOptions.searchCommandOptions.dataModelOptions.include);
            queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, panelsCommandOptions.searchCommandOptions.dataModelOptions.exclude);
            queryOptions.put(QueryOptions.LIMIT, panelsCommandOptions.searchCommandOptions.numericOptions.limit);
            queryOptions.put(QueryOptions.SKIP, panelsCommandOptions.searchCommandOptions.numericOptions.skip);

            return openCGAClient.getPanelClient().search(query, queryOptions);
        }
    }


    private QueryResponse<DiseasePanelAclEntry> updateAcl() throws IOException, CatalogException {
        AclCommandOptions.AclsUpdateCommandOptions commandOptions = panelsCommandOptions.aclsUpdateCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotNull("study", commandOptions.study);

        ObjectMap bodyParams = new ObjectMap();
        bodyParams.putIfNotNull("permissions", commandOptions.permissions);
        bodyParams.putIfNotNull("action", commandOptions.action);
        bodyParams.putIfNotNull("panel", commandOptions.id);

        return openCGAClient.getPanelClient().updateAcl(commandOptions.memberId, queryParams, bodyParams);
    }
}
