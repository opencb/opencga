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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.StudyCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogStudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Study;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class StudiesCommandExecutor extends OpencgaCommandExecutor {

    private StudyCommandOptions studiesCommandOptions;

    public StudiesCommandExecutor(StudyCommandOptions studiesCommandOptions) {
        super(studiesCommandOptions.commonCommandOptions);
        this.studiesCommandOptions = studiesCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing studies command line");

        String subCommandString = getParsedSubCommand(studiesCommandOptions.jCommander);
        switch (subCommandString) {
            case "create":
                create();
                break;
            case "info":
                info();
                break;
            case "search":
                search();
                break;
            case "scan-files":
                scanFiles();
                break;
            case "files":
                files();
                break;
            case "update":
                update();
                break;
            case "delete":
                delete();
                break;
            case "summary":
                summary();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void create() throws CatalogException, IOException {

        logger.debug("Creating a new study");
        String alias = studiesCommandOptions.createCommandOptions.alias;
        String name = studiesCommandOptions.createCommandOptions.name;
        String projectId = studiesCommandOptions.createCommandOptions.projectId;

        String description = studiesCommandOptions.createCommandOptions.description;
        String status = studiesCommandOptions.createCommandOptions.status;
        ObjectMap o = new ObjectMap();
        o.append(CatalogStudyDBAdaptor.QueryParams.DESCRIPTION.key(),description);
        o.append(CatalogStudyDBAdaptor.QueryParams.STATUS_STATUS.key(),status);
        openCGAClient.getStudyClient().create(projectId, name, alias, o);
        /************************************************************************************************ OJO, ULTIMO PARAMETRO PARAMS
         * ES CORRECTO?? JUM */
        System.out.println("Done.");
    }

    private void info() throws CatalogException, IOException {

        logger.debug("Getting the project info");
        QueryResponse<Study> info = openCGAClient.getStudyClient().get(studiesCommandOptions.infoCommandOptions.id, null);
        System.out.println("Study: " + info);
    }

    private void scanFiles() throws CatalogException, IOException {

        logger.debug("Scan the study folder to find changes\n");
        QueryResponse scan =  openCGAClient.getStudyClient().scanFiles(studiesCommandOptions.scanFilesCommandOptions.id, null);
        System.out.println("Scan Files: " + scan);
    }

    private void files() throws CatalogException, IOException {

        logger.debug("Listing files in folder");
        //QueryResponse<File> files = openCGAClient.getStudyClient().getFiles(studiesCommandOptions.listCommandOptions.id, null);
        //System.out.println("Files= " + files);
    }

    private void update() throws CatalogException, IOException {
        //TODO
        logger.debug("Updating the study");
        String id = studiesCommandOptions.updateCommandOptions.id;
        String name = studiesCommandOptions.updateCommandOptions.name;
        Study.Type type = studiesCommandOptions.updateCommandOptions.type;
        String description = studiesCommandOptions.updateCommandOptions.description;
        String status = studiesCommandOptions.updateCommandOptions.status;

       // QueryResponse

    }

    private void delete() throws CatalogException, IOException {

        logger.debug("Deleting a study");


    }

    private void summary() throws CatalogException, IOException {

        logger.debug("Doing summary with the general stats of a study");


    }

    private void search() throws CatalogException, IOException {

        //search(Query query, QueryOptions options)
        logger.debug("Searching study");

        Query query = new Query();

        String id = studiesCommandOptions.searchCommandOptions.id;
        String projectId = studiesCommandOptions.searchCommandOptions.projectId;
        String name = studiesCommandOptions.searchCommandOptions.name;
        String alias = studiesCommandOptions.searchCommandOptions.alias;
        String type = studiesCommandOptions.searchCommandOptions.type;
        String creationDate = studiesCommandOptions.searchCommandOptions.creationDate;
        String status = studiesCommandOptions.searchCommandOptions.status;
        String attributes = studiesCommandOptions.searchCommandOptions.attributes;
        String nattributes = studiesCommandOptions.searchCommandOptions.nattributes;
        boolean battributes = studiesCommandOptions.searchCommandOptions.battributes;
        String groups = studiesCommandOptions.searchCommandOptions.groups;
        String groupsUsers = studiesCommandOptions.searchCommandOptions.groupsUsers;


        if (id != null && !id.isEmpty()) {
            query.put(CatalogStudyDBAdaptor.QueryParams.ID.key(), id );
        }

        if (projectId != null && !projectId.isEmpty()) {
            query.put(CatalogStudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId);
        }

        if (name != null && !name.isEmpty()) {
            query.put(CatalogStudyDBAdaptor.QueryParams.NAME.key(), name);
        }

        if (alias != null && !alias.isEmpty()) {
            query.put(CatalogStudyDBAdaptor.QueryParams.ALIAS.key(), alias);
        }

        if (type != null && !type.isEmpty()) {
            query.put(CatalogStudyDBAdaptor.QueryParams.TYPE.key(), type);
        }

        if (creationDate != null && !creationDate.isEmpty()) {
            query.put(CatalogStudyDBAdaptor.QueryParams.CREATION_DATE.key(), creationDate);
        }

        if (status != null && !status.isEmpty()) {
            query.put(CatalogStudyDBAdaptor.QueryParams.STATUS_STATUS.key(), status);
        }

        if (attributes != null && !attributes.isEmpty()) {
            query.put(CatalogStudyDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);
        }

        if (nattributes != null && !nattributes.isEmpty()) {
            query.put(CatalogStudyDBAdaptor.QueryParams.NATTRIBUTES.key(), nattributes);
        }

        if (battributes) {
            query.put(CatalogStudyDBAdaptor.QueryParams.BATTRIBUTES.key(), battributes);
        }

        if (groups != null && !groups.isEmpty()) {
            query.put(CatalogStudyDBAdaptor.QueryParams.GROUPS.key(), groups);
        }

        if (groupsUsers != null && !groupsUsers.isEmpty()) {
            query.put(CatalogStudyDBAdaptor.QueryParams.GROUP_USER_IDS.key(), groupsUsers);
        }

        QueryResponse<Study> studies = openCGAClient.getStudyClient().search(query, null);
        System.out.println("Studies: " + studies);
    }
}
