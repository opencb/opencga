/*
 * Copyright 2015-2016 OpenCB
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
        logger.debug("Executing Project command line");

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
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    private QueryResponse<Project> create() throws CatalogException, IOException {
        logger.debug("Creating a new project");

        ObjectMap params = new ObjectMap();
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.DESCRIPTION.key(), projectsCommandOptions.createCommandOptions.description);
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.ORGANIZATION.key(), projectsCommandOptions.createCommandOptions.organization);
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.ORGANISM_SCIENTIFIC_NAME.key(), projectsCommandOptions.createCommandOptions.scientificName);
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key(), projectsCommandOptions.createCommandOptions.commonName);
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.ORGANISM_TAXONOMY_CODE.key(), projectsCommandOptions.createCommandOptions.taxonomyCode);
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key(), projectsCommandOptions.createCommandOptions.assembly);

        return openCGAClient.getProjectClient().create(projectsCommandOptions.createCommandOptions.name,
                projectsCommandOptions.createCommandOptions.alias, params);
    }

    private QueryResponse<Project> info() throws CatalogException, IOException {
        logger.debug("Getting the project info");

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, projectsCommandOptions.infoCommandOptions.dataModelOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, projectsCommandOptions.infoCommandOptions.dataModelOptions.exclude);
        return openCGAClient.getProjectClient().get(projectsCommandOptions.infoCommandOptions.project, null);
    }

    private QueryResponse<Project> update() throws CatalogException, IOException {
        logger.debug("Updating project");

        ObjectMap params = new ObjectMap();
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.NAME.key(), projectsCommandOptions.updateCommandOptions.name);
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.DESCRIPTION.key(), projectsCommandOptions.updateCommandOptions.description);
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.ORGANIZATION.key(), projectsCommandOptions.updateCommandOptions.organization);
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.ATTRIBUTES.key(), projectsCommandOptions.updateCommandOptions.attributes);
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.ORGANISM_SCIENTIFIC_NAME.key(),
                projectsCommandOptions.updateCommandOptions.scientificName);
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key(),
                projectsCommandOptions.updateCommandOptions.commonName);
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.ORGANISM_TAXONOMY_CODE.key(),
                projectsCommandOptions.updateCommandOptions.taxonomyCode);
        params.putIfNotNull(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key(), projectsCommandOptions.updateCommandOptions.assembly);

        return openCGAClient.getProjectClient().update(projectsCommandOptions.updateCommandOptions.project, params);
    }

    private QueryResponse<Project> delete() throws CatalogException, IOException {
        logger.debug("Deleting project ");

        ObjectMap objectMap = new ObjectMap();
        return openCGAClient.getProjectClient().delete(projectsCommandOptions.deleteCommandOptions.project, objectMap);
    }

    private QueryResponse<Study> studies() throws CatalogException, IOException {
        logger.debug("Getting all studies the from a project ");

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotNull(QueryOptions.INCLUDE, projectsCommandOptions.studiesCommandOptions.dataModelOptions.include);
        queryOptions.putIfNotNull(QueryOptions.EXCLUDE,projectsCommandOptions.studiesCommandOptions.dataModelOptions.exclude);
        queryOptions.putIfNotNull(QueryOptions.LIMIT, projectsCommandOptions.studiesCommandOptions.numericOptions.limit);
        queryOptions.putIfNotNull(QueryOptions.SKIP, projectsCommandOptions.studiesCommandOptions.numericOptions.skip);
        return openCGAClient.getProjectClient().getStudies(projectsCommandOptions.studiesCommandOptions.project, queryOptions);
    }

}
