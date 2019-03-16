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


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.ProjectCommandOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.Study;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class ProjectCommandExecutor extends OpencgaCommandExecutor {
    // TODO: Add include/exclude/skip/... (queryOptions) to the client calls !!!!

    private ProjectCommandOptions projectsCommandOptions;

    public ProjectCommandExecutor(ProjectCommandOptions projectsCommandOptions) {
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
            case "search":
                queryResponse = search();
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

        ProjectCommandOptions.CreateCommandOptions commandOptions = projectsCommandOptions.createCommandOptions;

        ObjectMap params = new ObjectMap();
        // First we populate the organism information using the client configuration
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.NAME.key(), commandOptions.name);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ID.key(), commandOptions.id);
        params.putIfNotEmpty("alias", commandOptions.alias);

        Project.Organism organism = clientConfiguration.getOrganism();
        organism.setAssembly(StringUtils.isNotEmpty(commandOptions.assembly) ? commandOptions.assembly : organism.getAssembly());
        organism.setCommonName(StringUtils.isNotEmpty(commandOptions.commonName) ? commandOptions.commonName : organism.getCommonName());
        organism.setScientificName(StringUtils.isNotEmpty(commandOptions.scientificName)
                ? commandOptions.scientificName : organism.getScientificName());
        organism.setTaxonomyCode(StringUtils.isNotEmpty(commandOptions.taxonomyCode)
                ? Integer.parseInt(commandOptions.taxonomyCode) : organism.getTaxonomyCode());
        params.put(ProjectDBAdaptor.QueryParams.ORGANISM.key(), organism);

        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.DESCRIPTION.key(), commandOptions.description);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ORGANIZATION.key(), commandOptions.organization);

        return openCGAClient.getProjectClient().create(params);
    }

    private QueryResponse<Project> info() throws CatalogException, IOException {
        logger.debug("Getting the project info");

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, projectsCommandOptions.infoCommandOptions.dataModelOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, projectsCommandOptions.infoCommandOptions.dataModelOptions.exclude);
        return openCGAClient.getProjectClient().get(projectsCommandOptions.infoCommandOptions.project, queryOptions);
    }

    private QueryResponse<Project> search() throws CatalogException, IOException {
        logger.debug("Search projects");

        ProjectCommandOptions.SearchCommandOptions commandOptions = projectsCommandOptions.searchCommandOptions;

        Query query = new Query();
        query.putIfNotEmpty("owner", commandOptions.owner);
        query.putIfNotEmpty(ProjectDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        query.putIfNotEmpty(ProjectDBAdaptor.QueryParams.NAME.key(), commandOptions.name);
        query.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ID.key(), commandOptions.alias);
        query.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ORGANIZATION.key(), commandOptions.organization);
        query.putIfNotEmpty(ProjectDBAdaptor.QueryParams.DESCRIPTION.key(), commandOptions.description);
        query.putIfNotEmpty(ProjectDBAdaptor.QueryParams.CREATION_DATE.key(), commandOptions.creationDate);
        query.putIfNotEmpty("status", commandOptions.status);
        query.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ATTRIBUTES.key(), commandOptions.attributes);
        query.putAll(commandOptions.commonOptions.params);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, projectsCommandOptions.infoCommandOptions.dataModelOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, projectsCommandOptions.infoCommandOptions.dataModelOptions.exclude);
        return openCGAClient.getProjectClient().search(query, queryOptions);
    }


    private QueryResponse<Project> update() throws CatalogException, IOException {
        logger.debug("Updating project");

        ProjectCommandOptions.UpdateCommandOptions commandOptions = projectsCommandOptions.updateCommandOptions;

        ObjectMap params;
        if (StringUtils.isNotEmpty(commandOptions.json)) {
            params = loadFile(commandOptions.json);
        } else {
            params = new ObjectMap();
            params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.NAME.key(), projectsCommandOptions.updateCommandOptions.name);
            params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.DESCRIPTION.key(), projectsCommandOptions.updateCommandOptions.description);
            params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ORGANIZATION.key(), projectsCommandOptions.updateCommandOptions.organization);
            params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ATTRIBUTES.key(), projectsCommandOptions.updateCommandOptions.attributes);
            params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key(),
                    projectsCommandOptions.updateCommandOptions.commonName);
            params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ORGANISM_TAXONOMY_CODE.key(),
                    projectsCommandOptions.updateCommandOptions.taxonomyCode);
        }

        return openCGAClient.getProjectClient().update(projectsCommandOptions.updateCommandOptions.project, null, params);
    }

    private QueryResponse<Project> delete() throws CatalogException, IOException {
        logger.debug("Deleting project ");

        return openCGAClient.getProjectClient().delete(projectsCommandOptions.deleteCommandOptions.project, new ObjectMap());
    }

    private QueryResponse<Study> studies() throws CatalogException, IOException {
        logger.debug("Getting all studies the from a project ");

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, projectsCommandOptions.studiesCommandOptions.dataModelOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE,projectsCommandOptions.studiesCommandOptions.dataModelOptions.exclude);
        queryOptions.put(QueryOptions.LIMIT, projectsCommandOptions.studiesCommandOptions.numericOptions.limit);
        queryOptions.put(QueryOptions.SKIP, projectsCommandOptions.studiesCommandOptions.numericOptions.skip);
        return openCGAClient.getProjectClient().getStudies(projectsCommandOptions.studiesCommandOptions.project, queryOptions);
    }

}
