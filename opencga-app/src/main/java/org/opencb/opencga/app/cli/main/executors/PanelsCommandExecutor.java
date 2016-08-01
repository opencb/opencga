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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.options.PanelCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogPanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.DiseasePanel;
import org.opencb.opencga.catalog.models.acls.DiseasePanelAclEntry;
import org.opencb.opencga.client.rest.PanelClient;

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
        switch (subCommandString) {
            case "create":
                create();
                break;
            case "info":
                info();
                break;
            case "acl":
                aclCommandExecutor.acls(panelsCommandOptions.aclsCommandOptions, openCGAClient.getPanelClient());
                break;
            case "acl-create":
                aclCommandExecutor.aclsCreate(panelsCommandOptions.aclsCreateCommandOptions, openCGAClient.getPanelClient());
                break;
            case "acl-member-delete":
                aclCommandExecutor.aclMemberDelete(panelsCommandOptions.aclsMemberDeleteCommandOptions, openCGAClient.getPanelClient());
                break;
            case "acl-member-info":
                aclCommandExecutor.aclMemberInfo(panelsCommandOptions.aclsMemberInfoCommandOptions, openCGAClient.getPanelClient());
                break;
            case "acl-member-update":
                aclCommandExecutor.aclMemberUpdate(panelsCommandOptions.aclsMemberUpdateCommandOptions, openCGAClient.getPanelClient());
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    /**********************************************  Administration Commands  ***********************************************/
    private void create() throws CatalogException, IOException {
        logger.debug("Creating a new panel");
        String name = panelsCommandOptions.createCommandOptions.name;
        String disease = panelsCommandOptions.createCommandOptions.disease;

        ObjectMap o = new ObjectMap();
        if (StringUtils.isNotEmpty(panelsCommandOptions.createCommandOptions.description)) {
            o.append(CatalogPanelDBAdaptor.QueryParams.DESCRIPTION.key(), panelsCommandOptions.createCommandOptions.description);
        }
        if (StringUtils.isNotEmpty(panelsCommandOptions.createCommandOptions.genes)) {
            o.append(CatalogPanelDBAdaptor.QueryParams.GENES.key(), panelsCommandOptions.createCommandOptions.genes);
        }
        if (StringUtils.isNotEmpty(panelsCommandOptions.createCommandOptions.regions)) {
            o.append(CatalogPanelDBAdaptor.QueryParams.REGIONS.key(), panelsCommandOptions.createCommandOptions.regions);
        }
        if (StringUtils.isNotEmpty(panelsCommandOptions.createCommandOptions.variants)) {
            o.append(CatalogPanelDBAdaptor.QueryParams.VARIANTS.key(), panelsCommandOptions.createCommandOptions.variants);
        }


        openCGAClient.getPanelClient().create(panelsCommandOptions.createCommandOptions.studyId, name, disease, o);

        System.out.println("Done.");
    }

    private void info() throws CatalogException, IOException  {
        logger.debug("Getting panel information");
        QueryOptions o = new QueryOptions();
        if (StringUtils.isNotEmpty(panelsCommandOptions.infoCommandOptions.commonOptions.include)) {
            o.append(QueryOptions.INCLUDE, panelsCommandOptions.infoCommandOptions.commonOptions.include);
        }
        if (StringUtils.isNotEmpty(panelsCommandOptions.infoCommandOptions.commonOptions.exclude)) {
            o.append(QueryOptions.EXCLUDE, panelsCommandOptions.infoCommandOptions.commonOptions.exclude);
        }
        openCGAClient.getPanelClient().get(panelsCommandOptions.createCommandOptions.studyId, o);
    }
}
