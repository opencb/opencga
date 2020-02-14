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
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.PanelCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelAclUpdateParams;
import org.opencb.opencga.core.response.RestResponse;

/**
 * Created by sgallego on 6/15/16.
 */
public class PanelCommandExecutor extends OpencgaCommandExecutor {

    private PanelCommandOptions panelsCommandOptions;

    public PanelCommandExecutor(PanelCommandOptions panelsCommandOptions) {
        super(panelsCommandOptions.commonCommandOptions);
        this.panelsCommandOptions = panelsCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing panel command line");

        String subCommandString = getParsedSubCommand(panelsCommandOptions.jCommander);
        RestResponse queryResponse = null;
        switch (subCommandString) {
//            case "create":
//                queryResponse = create();
//                break;
            case "info":
                queryResponse = info();
                break;
            case "search":
                queryResponse = search();
                break;
            case "acl":
                queryResponse = acl();
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
//    private RestResponse<DiseasePanel> create() throws CatalogException, IOException {
//        logger.debug("Creating a new panel");
//        String name = panelsCommandOptions.createCommandOptions.name;
//        String disease = panelsCommandOptions.createCommandOptions.disease;
//
//        ObjectMap params = new ObjectMap();
//        params.putIfNotEmpty(DiseasePanelDBAdaptor.QueryParams.DESCRIPTION.key(), panelsCommandOptions.createCommandOptions.description);
//        params.putIfNotEmpty(DiseasePanelDBAdaptor.QueryParams.GENES.key(), panelsCommandOptions.createCommandOptions.genes);
//        params.putIfNotEmpty(DiseasePanelDBAdaptor.QueryParams.REGIONS.key(), panelsCommandOptions.createCommandOptions.regions);
//        params.putIfNotEmpty(DiseasePanelDBAdaptor.QueryParams.VARIANTS.key(), panelsCommandOptions.createCommandOptions.variants);
//        return openCGAClient.getPanelClient().create(resolveStudy(panelsCommandOptions.createCommandOptions.studyId), name, disease, params);
//    }

    private RestResponse<Panel> info() throws ClientException {
        logger.debug("Getting panel information");

        PanelCommandOptions.InfoCommandOptions c = panelsCommandOptions.infoCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("study", resolveStudy(c.study));
        params.putIfNotEmpty(QueryOptions.INCLUDE, c.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, c.dataModelOptions.exclude);
        return openCGAClient.getDiseasePanelClient().info(c.id, params);
    }

    private RestResponse<Panel> search() throws ClientException  {
        logger.debug("Searching panels");

        PanelCommandOptions.SearchCommandOptions c = panelsCommandOptions.searchCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("study", resolveStudy(c.study));
        params.putIfNotNull(PanelDBAdaptor.QueryParams.NAME.key(), c.name);
        params.putIfNotNull(PanelDBAdaptor.QueryParams.PHENOTYPES.key(), c.phenotypes);
        params.putIfNotNull(PanelDBAdaptor.QueryParams.VARIANTS.key(), c.variants);
        params.putIfNotNull(PanelDBAdaptor.QueryParams.REGIONS.key(), c.regions);
        params.putIfNotNull(PanelDBAdaptor.QueryParams.GENES.key(), c.genes);
        params.putIfNotNull(PanelDBAdaptor.QueryParams.DESCRIPTION.key(), c.description);
        params.putIfNotNull(PanelDBAdaptor.QueryParams.AUTHOR.key(), c.author);
        params.putIfNotNull(PanelDBAdaptor.QueryParams.TAGS.key(), c.tags);
        params.putIfNotNull(PanelDBAdaptor.QueryParams.CATEGORIES.key(), c.categories);
        params.putIfNotNull(PanelDBAdaptor.QueryParams.CREATION_DATE.key(), c.creationDate);
        params.putIfNotNull(PanelDBAdaptor.QueryParams.RELEASE.key(), c.release);
        params.putIfNotNull(PanelDBAdaptor.QueryParams.SNAPSHOT.key(), c.snapshot);

        params.put(QueryOptions.COUNT, c.numericOptions.count);
        params.putIfNotEmpty(QueryOptions.INCLUDE, c.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, c.dataModelOptions.exclude);
        params.put(QueryOptions.LIMIT, c.numericOptions.limit);
        params.put(QueryOptions.SKIP, c.numericOptions.skip);

        return openCGAClient.getDiseasePanelClient().search(params);
    }


    private RestResponse<ObjectMap> updateAcl() throws ClientException, CatalogException {
        AclCommandOptions.AclsUpdateCommandOptions commandOptions = panelsCommandOptions.aclsUpdateCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotNull("study", commandOptions.study);

        PanelAclUpdateParams aclUpdateParams = new PanelAclUpdateParams()
                .setPanel(extractIdsFromListOrFile(commandOptions.id))
                .setPermissions(commandOptions.permissions)
                .setAction(commandOptions.action);

        return openCGAClient.getDiseasePanelClient().updateAcl(commandOptions.memberId, aclUpdateParams, queryParams);
    }

    private RestResponse<ObjectMap> acl() throws ClientException {
        AclCommandOptions.AclsCommandOptions commandOptions = panelsCommandOptions.aclsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
        params.putIfNotEmpty("member", commandOptions.memberId);

        params.putAll(commandOptions.commonOptions.params);

        return openCGAClient.getDiseasePanelClient().acl(commandOptions.id, params);
    }
}
