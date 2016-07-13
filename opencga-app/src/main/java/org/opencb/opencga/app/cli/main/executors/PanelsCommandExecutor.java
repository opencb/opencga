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
import org.opencb.opencga.app.cli.main.options.PanelCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogPanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.acls.DiseasePanelAclEntry;
import org.opencb.opencga.client.rest.PanelClient;

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
            case "acl":
                acls();
                break;
            case "acl-create":
                aclsCreate();
                break;
            case "acl-member-delete":
                aclMemberDelete();
                break;
            case "acl-member-info":
                aclMemberInfo();
                break;
            case "acl-member-update":
                aclMemberUpdate();
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
    /********************************************  Administration ACL commands  ***********************************************/

    private void acls() throws CatalogException,IOException {

        logger.debug("Acls");
        ObjectMap objectMap = new ObjectMap();
        QueryResponse<DiseasePanelAclEntry> acls = openCGAClient.getPanelClient().getAcls(panelsCommandOptions.aclsCommandOptions.id);

        System.out.println(acls.toString());

    }
    private void aclsCreate() throws CatalogException,IOException{

        logger.debug("Creating acl");

        QueryOptions queryOptions = new QueryOptions();

        /*if (StringUtils.isNotEmpty(studiesCommandOptions.aclsCreateCommandOptions.templateId)) {
            queryOptions.put(CatalogStudyDBAdaptor.QueryParams.TEMPLATE_ID.key(), studiesCommandOptions.aclsCreateCommandOptions.templateId);
        }*/

        QueryResponse<DiseasePanelAclEntry> acl =
                openCGAClient.getPanelClient().createAcl(panelsCommandOptions.aclsCreateCommandOptions.id,
                        panelsCommandOptions.aclsCreateCommandOptions.permissions, panelsCommandOptions.aclsCreateCommandOptions.members,
                        queryOptions);
        System.out.println(acl.toString());
    }
    private void aclMemberDelete() throws CatalogException,IOException {

        logger.debug("Creating acl");

        QueryOptions queryOptions = new QueryOptions();
        QueryResponse<Object> acl = openCGAClient.getPanelClient().deleteAcl(panelsCommandOptions.aclsMemberDeleteCommandOptions.id,
                panelsCommandOptions.aclsMemberDeleteCommandOptions.memberId, queryOptions);
        System.out.println(acl.toString());
    }
    private void aclMemberInfo() throws CatalogException,IOException {

        logger.debug("Creating acl");

        QueryResponse<DiseasePanelAclEntry> acls = openCGAClient.getPanelClient().getAcl(panelsCommandOptions.aclsMemberInfoCommandOptions.id,
                panelsCommandOptions.aclsMemberInfoCommandOptions.memberId);
        System.out.println(acls.toString());
    }

    private void aclMemberUpdate() throws CatalogException,IOException {

        logger.debug("Updating acl");

        ObjectMap objectMap = new ObjectMap();
        if (StringUtils.isNotEmpty(panelsCommandOptions.aclsMemberUpdateCommandOptions.addPermissions)) {
            objectMap.put(PanelClient.AclParams.ADD_PERMISSIONS.key(), panelsCommandOptions.aclsMemberUpdateCommandOptions.addPermissions);
        }
        if (StringUtils.isNotEmpty(panelsCommandOptions.aclsMemberUpdateCommandOptions.removePermissions)) {
            objectMap.put(PanelClient.AclParams.REMOVE_PERMISSIONS.key(), panelsCommandOptions.aclsMemberUpdateCommandOptions.removePermissions);
        }
        if (StringUtils.isNotEmpty(panelsCommandOptions.aclsMemberUpdateCommandOptions.setPermissions)) {
            objectMap.put(PanelClient.AclParams.SET_PERMISSIONS.key(), panelsCommandOptions.aclsMemberUpdateCommandOptions.setPermissions);
        }

        QueryResponse<DiseasePanelAclEntry> acl = openCGAClient.getPanelClient().updateAcl(panelsCommandOptions.aclsMemberUpdateCommandOptions.id,
                panelsCommandOptions.aclsMemberUpdateCommandOptions.memberId, objectMap);
        System.out.println(acl.toString());
    }
}
