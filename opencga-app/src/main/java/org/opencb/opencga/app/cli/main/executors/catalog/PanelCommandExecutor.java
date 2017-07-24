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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.options.PanelCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.DiseasePanel;
import org.opencb.opencga.catalog.models.acls.permissions.DiseasePanelAclEntry;

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
        params.putIfNotEmpty(PanelDBAdaptor.QueryParams.DESCRIPTION.key(), panelsCommandOptions.createCommandOptions.description);
        params.putIfNotEmpty(PanelDBAdaptor.QueryParams.GENES.key(), panelsCommandOptions.createCommandOptions.genes);
        params.putIfNotEmpty(PanelDBAdaptor.QueryParams.REGIONS.key(), panelsCommandOptions.createCommandOptions.regions);
        params.putIfNotEmpty(PanelDBAdaptor.QueryParams.VARIANTS.key(), panelsCommandOptions.createCommandOptions.variants);
        return openCGAClient.getPanelClient().create(resolveStudy(panelsCommandOptions.createCommandOptions.studyId), name, disease, params);
    }

    private QueryResponse<DiseasePanel> info() throws CatalogException, IOException  {
        logger.debug("Getting panel information");

        QueryOptions options = new QueryOptions();
        options.putIfNotEmpty(QueryOptions.INCLUDE, panelsCommandOptions.infoCommandOptions.include);
        options.putIfNotEmpty(QueryOptions.EXCLUDE, panelsCommandOptions.infoCommandOptions.exclude);
        return openCGAClient.getPanelClient().get(panelsCommandOptions.infoCommandOptions.id, options);
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
