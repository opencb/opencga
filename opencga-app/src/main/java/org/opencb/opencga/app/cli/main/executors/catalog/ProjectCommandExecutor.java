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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.ProjectCommandOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.RestResponse;

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
        RestResponse queryResponse = null;
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
            case "studies":
                queryResponse = studies();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    private RestResponse<Project> create() throws ClientException {
        logger.debug("Creating a new project");

        ProjectCommandOptions.CreateCommandOptions commandOptions = projectsCommandOptions.createCommandOptions;

        Project.Organism organism = clientConfiguration.getOrganism();
        organism.setAssembly(StringUtils.isNotEmpty(commandOptions.assembly) ? commandOptions.assembly : organism.getAssembly());
        organism.setCommonName(StringUtils.isNotEmpty(commandOptions.commonName) ? commandOptions.commonName : organism.getCommonName());
        organism.setScientificName(StringUtils.isNotEmpty(commandOptions.scientificName)
                ? commandOptions.scientificName : organism.getScientificName());
        organism.setTaxonomyCode(StringUtils.isNotEmpty(commandOptions.taxonomyCode)
                ? Integer.parseInt(commandOptions.taxonomyCode) : organism.getTaxonomyCode());

        ProjectCreateParams createParams = new ProjectCreateParams()
                .setId(commandOptions.id)
                .setName(commandOptions.name)
                .setDescription(commandOptions.description)
                .setOrganization(commandOptions.organization)
                .setOrganism(organism);

        return openCGAClient.getProjectClient().create(createParams);
    }

    private RestResponse<Project> info() throws ClientException {
        logger.debug("Getting the project info");

        ProjectCommandOptions.InfoCommandOptions c = projectsCommandOptions.infoCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(QueryOptions.INCLUDE, c.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, c.dataModelOptions.exclude);
        return openCGAClient.getProjectClient().info(c.project, params);
    }

    private RestResponse<Project> search() throws ClientException {
        logger.debug("Search projects");

        ProjectCommandOptions.SearchCommandOptions commandOptions = projectsCommandOptions.searchCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("owner", commandOptions.owner);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.NAME.key(), commandOptions.name);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ID.key(), commandOptions.alias);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ORGANIZATION.key(), commandOptions.organization);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.DESCRIPTION.key(), commandOptions.description);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.CREATION_DATE.key(), commandOptions.creationDate);
        params.putIfNotEmpty("status", commandOptions.status);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ATTRIBUTES.key(), commandOptions.attributes);
        params.putAll(commandOptions.commonOptions.params);

        params.putIfNotEmpty(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);

        return openCGAClient.getProjectClient().search(params);
    }


    private RestResponse<Project> update() throws ClientException {
        logger.debug("Updating project");

        ProjectCommandOptions.UpdateCommandOptions commandOptions = projectsCommandOptions.updateCommandOptions;

        Project.Organism organism = null;
        if (commandOptions.taxonomyCode != null || StringUtils.isNotEmpty(commandOptions.commonName)) {
            organism = new Project.Organism()
                    .setCommonName(commandOptions.commonName);
            if (commandOptions.taxonomyCode != null) {
                organism.setTaxonomyCode(commandOptions.taxonomyCode);
            }
        }

        ProjectUpdateParams params = new ProjectUpdateParams()
                .setName(commandOptions.name)
                .setDescription(commandOptions.description)
                .setOrganization(commandOptions.organization)
                .setOrganism(organism);

        return openCGAClient.getProjectClient().update(commandOptions.project, params);
    }

    private RestResponse<Study> studies() throws ClientException {
        logger.debug("Getting all studies the from a project ");

        ProjectCommandOptions.StudiesCommandOptions commandOptions = projectsCommandOptions.studiesCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);
        params.put(QueryOptions.LIMIT, commandOptions.numericOptions.limit);
        params.put(QueryOptions.SKIP, commandOptions.numericOptions.skip);

        return openCGAClient.getProjectClient().studies(commandOptions.project, params);
    }

}
