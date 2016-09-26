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
import org.opencb.opencga.app.cli.main.options.catalog.ProjectCommandOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Study;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class ProjectsCommandExecutor extends OpencgaCommandExecutor {
    // TODO: Add include/exclude/skip/... (queryOptions) to the client calls !!!!

    private ProjectCommandOptions projectsCommandOptions;

    public ProjectsCommandExecutor(ProjectCommandOptions projectsCommandOptions) {
        super(projectsCommandOptions.commonCommandOptions);
        this.projectsCommandOptions = projectsCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = getParsedSubCommand(projectsCommandOptions.jCommander);
        QueryResponse queryResponse = null;
        switch (subCommandString) {
            case "create":
                queryResponse = create();
                break;
            case "info":
                queryResponse = info();
                break;
            case "update":
                queryResponse = update();
                break;
            case "delete":
                queryResponse = delete();
                break;
            case "studies":
                queryResponse = studies();
                break;
            case "help":
                queryResponse = help();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);

    }

    private QueryResponse<Project> create() throws CatalogException, IOException {
        logger.debug("Creating a new project");
        ObjectMap o = new ObjectMap();
        o.append(ProjectDBAdaptor.QueryParams.DESCRIPTION.key(), projectsCommandOptions.createCommandOptions.description);
        if (projectsCommandOptions.createCommandOptions.organization != null) {
            o.append("organization", projectsCommandOptions.createCommandOptions.organization);
        }
        return openCGAClient.getProjectClient().create(projectsCommandOptions.createCommandOptions.name,
                projectsCommandOptions.createCommandOptions.alias, o);
    }

    private QueryResponse<Project> info() throws CatalogException, IOException {
        logger.debug("Geting the project info");
        return openCGAClient.getProjectClient().get(projectsCommandOptions.infoCommandOptions.id, null);
    }

    private QueryResponse<Project> update() throws CatalogException, IOException {
        logger.debug("Updating project");

        ObjectMap objectMap = new ObjectMap();

        objectMap.putIfNotEmpty(ProjectDBAdaptor.QueryParams.NAME.key(), projectsCommandOptions.updateCommandOptions.name);
        objectMap.putIfNotEmpty(ProjectDBAdaptor.QueryParams.DESCRIPTION.key(), projectsCommandOptions.updateCommandOptions.description);
        objectMap.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ORGANIZATION.key(), projectsCommandOptions.updateCommandOptions.organization);

        //if (StringUtils.isNotEmpty(projectsCommandOptions.updateCommandOptions.status)) {
        //    objectMap.put(CatalogProjectDBAdaptor.QueryParams.STATUS_NAME.key(), projectsCommandOptions.updateCommandOptions.status);
        //}
        objectMap.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ATTRIBUTES.key(), projectsCommandOptions.updateCommandOptions.attributes);

        return openCGAClient.getProjectClient().update(projectsCommandOptions.updateCommandOptions.id, objectMap);
    }

    private QueryResponse<Project> delete() throws CatalogException, IOException {
        logger.debug("Deleting project ");
        ObjectMap objectMap = new ObjectMap();
        return openCGAClient.getProjectClient().delete(projectsCommandOptions.deleteCommandOptions.id, objectMap);
    }

    private QueryResponse<Study> studies() throws CatalogException, IOException {
        logger.debug("Getting all studies the from a project ");
        QueryOptions queryOptions = new QueryOptions();

        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, projectsCommandOptions.studiesCommandOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE,projectsCommandOptions.studiesCommandOptions.exclude);
        queryOptions.putIfNotEmpty(QueryOptions.LIMIT, projectsCommandOptions.studiesCommandOptions.limit);
        queryOptions.putIfNotEmpty(QueryOptions.SKIP, projectsCommandOptions.studiesCommandOptions.skip);

        return openCGAClient.getProjectClient().getStudies(projectsCommandOptions.studiesCommandOptions.id, queryOptions);
    }

    private QueryResponse help() throws CatalogException, IOException {

        System.out.println("PENDING");
        return null;

    }

}
