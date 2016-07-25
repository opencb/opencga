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
import org.opencb.opencga.app.cli.main.options.ProjectCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogProjectDBAdaptor;
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
        switch (subCommandString) {
            case "create":
                create();
                break;
            case "info":
                info();
                break;
            case "update":
                update();
                break;
            case "delete":
                delete();
                break;
            case "studies":
                studies();
                break;
            case "help":
                help();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void create() throws CatalogException, IOException {
        logger.debug("Creating a new project");
        ObjectMap o = new ObjectMap();
        o.append(CatalogProjectDBAdaptor.QueryParams.DESCRIPTION.key(), projectsCommandOptions.createCommandOptions.description);
        if (projectsCommandOptions.createCommandOptions.organization != null) {
            o.append("organization", projectsCommandOptions.createCommandOptions.organization);
        }
        openCGAClient.getProjectClient().create(projectsCommandOptions.createCommandOptions.name,
                projectsCommandOptions.createCommandOptions.alias, o);

        System.out.println("Created");
    }

    private void info() throws CatalogException, IOException {
        logger.debug("Geting the project info");
        QueryResponse<Project> info = openCGAClient.getProjectClient().get(projectsCommandOptions.infoCommandOptions.id, null);
        System.out.println("Project: " + info);
    }

    private void update() throws CatalogException, IOException {
        logger.debug("Updating project");

        ObjectMap objectMap = new ObjectMap();

        if (StringUtils.isNotEmpty(projectsCommandOptions.updateCommandOptions.name)) {
            objectMap.put(CatalogProjectDBAdaptor.QueryParams.NAME.key(), projectsCommandOptions.updateCommandOptions.name);
        }

        if (StringUtils.isNotEmpty(projectsCommandOptions.updateCommandOptions.description)) {
            objectMap.put(CatalogProjectDBAdaptor.QueryParams.DESCRIPTION.key(), projectsCommandOptions.updateCommandOptions.description);
        }
        if (StringUtils.isNotEmpty(projectsCommandOptions.updateCommandOptions.organization)) {
            objectMap.put(CatalogProjectDBAdaptor.QueryParams.ORGANIZATION.key(), projectsCommandOptions.updateCommandOptions.organization);
        }
        if (StringUtils.isNotEmpty(projectsCommandOptions.updateCommandOptions.status)) {
            objectMap.put(CatalogProjectDBAdaptor.QueryParams.STATUS_NAME.key(), projectsCommandOptions.updateCommandOptions.status);
        }
        if (StringUtils.isNotEmpty(projectsCommandOptions.updateCommandOptions.attributes)) {
            objectMap.put(CatalogProjectDBAdaptor.QueryParams.ATTRIBUTES.key(), projectsCommandOptions.updateCommandOptions.attributes);
        }

        QueryResponse<Project> project = openCGAClient.getProjectClient().update(projectsCommandOptions.updateCommandOptions.id, objectMap);
        System.out.println("Project: " + project);

    }

    private void delete() throws CatalogException, IOException {
        logger.debug("Deleting project ");
        ObjectMap objectMap = new ObjectMap();
        QueryResponse<Project> study = openCGAClient.getProjectClient().delete(projectsCommandOptions.deleteCommandOptions.id, objectMap);
        System.out.println("Study: " + study);
    }

    private void studies() throws CatalogException, IOException {
        logger.debug("Getting all studies the from a project ");
        QueryOptions queryOptions = new QueryOptions();

        if (StringUtils.isNotEmpty(projectsCommandOptions.studiesCommandOptions.commonOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, projectsCommandOptions.studiesCommandOptions.commonOptions.include);
        }
        if (StringUtils.isNotEmpty(projectsCommandOptions.studiesCommandOptions.commonOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE,projectsCommandOptions.studiesCommandOptions.commonOptions.exclude);
        }
        if (StringUtils.isNotEmpty(projectsCommandOptions.studiesCommandOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, projectsCommandOptions.studiesCommandOptions.limit);
        }

        if (StringUtils.isNotEmpty(projectsCommandOptions.studiesCommandOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, projectsCommandOptions.studiesCommandOptions.skip);
        }
        QueryResponse<Study> study = openCGAClient.getProjectClient().getStudies(projectsCommandOptions.studiesCommandOptions.id, queryOptions);
        System.out.println("Study: " + study);

    }

    private void help() throws CatalogException, IOException {

        System.out.println("PENDING");

    }

}
