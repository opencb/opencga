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


import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.options.catalog.PanelCommandOptions;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.DiseasePanel;
import org.opencb.opencga.catalog.models.acls.permissions.DiseasePanelAclEntry;

import java.io.IOException;

/**
 * Created by sgallego on 6/15/16.
 */
public class PanelsCommandExecutor extends OpencgaCommandExecutor {

    private PanelCommandOptions panelsCommandOptions;
    private AclCommandExecutor<DiseasePanel, DiseasePanelAclEntry> aclCommandExecutor;

    public PanelsCommandExecutor(PanelCommandOptions panelsCommandOptions) {
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
            case "acl-create":
                queryResponse = aclCommandExecutor.aclsCreate(panelsCommandOptions.aclsCreateCommandOptions,
                        openCGAClient.getPanelClient());
                break;
            case "acl-member-delete":
                queryResponse = aclCommandExecutor.aclMemberDelete(panelsCommandOptions.aclsMemberDeleteCommandOptions,
                        openCGAClient.getPanelClient());
                break;
            case "acl-member-info":
                queryResponse = aclCommandExecutor.aclMemberInfo(panelsCommandOptions.aclsMemberInfoCommandOptions,
                        openCGAClient.getPanelClient());
                break;
            case "acl-member-update":
                queryResponse = aclCommandExecutor.aclMemberUpdate(panelsCommandOptions.aclsMemberUpdateCommandOptions,
                        openCGAClient.getPanelClient());
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

        ObjectMap o = new ObjectMap();

        o.putIfNotEmpty(PanelDBAdaptor.QueryParams.DESCRIPTION.key(), panelsCommandOptions.createCommandOptions.description);
        o.putIfNotEmpty(PanelDBAdaptor.QueryParams.GENES.key(), panelsCommandOptions.createCommandOptions.genes);
        o.putIfNotEmpty(PanelDBAdaptor.QueryParams.REGIONS.key(), panelsCommandOptions.createCommandOptions.regions);
        o.putIfNotEmpty(PanelDBAdaptor.QueryParams.VARIANTS.key(), panelsCommandOptions.createCommandOptions.variants);

        return openCGAClient.getPanelClient().create(panelsCommandOptions.createCommandOptions.studyId, name, disease, o);
    }

    private QueryResponse<DiseasePanel> info() throws CatalogException, IOException  {
        logger.debug("Getting panel information");
        QueryOptions o = new QueryOptions();

        o.putIfNotEmpty(QueryOptions.INCLUDE, panelsCommandOptions.infoCommandOptions.include);
        o.putIfNotEmpty(QueryOptions.EXCLUDE, panelsCommandOptions.infoCommandOptions.exclude);

        return openCGAClient.getPanelClient().get(panelsCommandOptions.infoCommandOptions.id, o);
    }
}
