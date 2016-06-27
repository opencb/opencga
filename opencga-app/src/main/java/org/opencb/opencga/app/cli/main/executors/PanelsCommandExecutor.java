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


import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.PanelCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogPanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;

import java.io.IOException;

/**
 * Created by sgallego on 6/15/16.
 */
public class PanelsCommandExecutor extends OpencgaCommandExecutor {

    private PanelCommandOptions panelsCommandOptions;

    public PanelsCommandExecutor(PanelCommandOptions panelsCommandOptions) {
        super(panelsCommandOptions.commonCommandOptions);
        this.panelsCommandOptions = panelsCommandOptions;
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
          /*  case "share":
                share();
                break;
            case "unshare":
                unshare();
                break;*/
            case "acl-create":
                aclCreate();
                break;
            case "assign-role":
                assignRole();
                break;
            case "remove-role":
                removeRole();
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
        String description = panelsCommandOptions.createCommandOptions.description;
        String genes = panelsCommandOptions.createCommandOptions.genes;
        String regions = panelsCommandOptions.createCommandOptions.regions;
        String variants = panelsCommandOptions.createCommandOptions.variants;

        ObjectMap o = new ObjectMap();
        o.append(CatalogPanelDBAdaptor.QueryParams.DESCRIPTION.key(),description);
        o.append(CatalogPanelDBAdaptor.QueryParams.GENES.key(),genes);
        o.append(CatalogPanelDBAdaptor.QueryParams.REGIONS.key(),regions);
        o.append(CatalogPanelDBAdaptor.QueryParams.VARIANTS.key(),variants);
        openCGAClient.getPanelClient().create(panelsCommandOptions.createCommandOptions.studyId, name, disease, o);

        System.out.println("Done.");
    }

    private void info() throws CatalogException, IOException  {
        logger.debug("Getting panel information");
        QueryOptions o = new QueryOptions();
        openCGAClient.getPanelClient().get(panelsCommandOptions.createCommandOptions.studyId, o);
    }

  /*  private void share() throws CatalogException {
        logger.debug("Sharing panel");
    }

    private void unshare() throws CatalogException {
        logger.debug("Unsharing panel");
    }*/

    /********************************************  Administration ACLS commands  ***********************************************/

    private void aclCreate() throws CatalogException,IOException{
        logger.debug("Creating acl");
    }
    private void removeRole() throws CatalogException,IOException{
        logger.debug("Removing role");

    }
    private void assignRole() throws CatalogException,IOException{
        logger.debug("Assigning role");
    }
}
