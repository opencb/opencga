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
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.project.ProjectUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.RestResponse;


public class ProjectCommandExecutor extends OpencgaCommandExecutor {

    private ProjectCommandOptions projectsCommandOptions;

    public ProjectCommandExecutor(ProjectCommandOptions projectsCommandOptions) {
        super(projectsCommandOptions.commonCommandOptions);

        this.projectsCommandOptions = projectsCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        RestResponse queryResponse = null;
        String subCommandString = getParsedSubCommand(projectsCommandOptions.jCommander);
        logger.debug("Executing Project subcommand '{}'", subCommandString);
        switch (subCommandString) {
            case "create":
                queryResponse = create();
                break;
            case "info":
                queryResponse = info();
                break;
            case "search":
                queryResponse = search();
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
        ProjectCommandOptions.CreateCommandOptions createCommandOptions = projectsCommandOptions.createCommandOptions;

        ProjectCreateParams createParams = new ProjectCreateParams()
                .setId(createCommandOptions.id)
                .setName(createCommandOptions.name)
                .setDescription(createCommandOptions.description)
                .setOrganism(new ProjectOrganism(createCommandOptions.scientificName, createCommandOptions.commonName,
                        createCommandOptions.assembly));
        return openCGAClient.getProjectClient().create(createParams);
    }

    private RestResponse<Project> info() throws ClientException {
        ProjectCommandOptions.InfoCommandOptions infoCommandOptions = projectsCommandOptions.infoCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(QueryOptions.INCLUDE, infoCommandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, infoCommandOptions.dataModelOptions.exclude);
        return openCGAClient.getProjectClient().info(infoCommandOptions.project, params);
    }

    private RestResponse<Project> search() throws ClientException {
        ProjectCommandOptions.SearchCommandOptions searchCommandOptions = projectsCommandOptions.searchCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("owner", searchCommandOptions.owner);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.STUDY.key(), searchCommandOptions.study);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.NAME.key(), searchCommandOptions.name);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ID.key(), searchCommandOptions.alias);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.DESCRIPTION.key(), searchCommandOptions.description);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.CREATION_DATE.key(), searchCommandOptions.creationDate);
        params.putIfNotEmpty("status", searchCommandOptions.status);
        params.putIfNotEmpty(ProjectDBAdaptor.QueryParams.ATTRIBUTES.key(), searchCommandOptions.attributes);
        params.putAll(searchCommandOptions.commonOptions.params);

        params.putIfNotEmpty(QueryOptions.INCLUDE, searchCommandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, searchCommandOptions.dataModelOptions.exclude);

        return openCGAClient.getProjectClient().search(params);
    }
    
    private RestResponse<Project> update() throws ClientException {
        ProjectCommandOptions.UpdateCommandOptions commandOptions = projectsCommandOptions.updateCommandOptions;

        ProjectOrganism organism = null;
        if (StringUtils.isNotEmpty(commandOptions.commonName)) {
            organism = new ProjectOrganism().setCommonName(commandOptions.commonName);
        }

        ProjectUpdateParams params = new ProjectUpdateParams()
                .setName(commandOptions.name)
                .setDescription(commandOptions.description)
                .setOrganism(organism);
        return openCGAClient.getProjectClient().update(commandOptions.project, params);
    }

    private RestResponse<Study> studies() throws ClientException {
        ProjectCommandOptions.StudiesCommandOptions studiesCommandOptions = projectsCommandOptions.studiesCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(QueryOptions.INCLUDE, studiesCommandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, studiesCommandOptions.dataModelOptions.exclude);
        params.put(QueryOptions.LIMIT, studiesCommandOptions.numericOptions.limit);
        params.put(QueryOptions.SKIP, studiesCommandOptions.numericOptions.skip);
        return openCGAClient.getProjectClient().studies(studiesCommandOptions.project, params);
    }

}
