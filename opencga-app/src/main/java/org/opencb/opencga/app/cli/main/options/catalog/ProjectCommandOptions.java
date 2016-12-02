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

package org.opencb.opencga.app.cli.main.options.catalog;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;

/**
 * Created by pfurio on 13/06/16.
 */
@Parameters(commandNames = {"projects"}, commandDescription = "Project commands")
public class ProjectCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public StudiesCommandOptions studiesCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public HelpCommandOptions helpCommandOptions;

    public JCommander jCommander;
    public GeneralCliOptions.CommonCommandOptions commonCommandOptions;

    public ProjectCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.studiesCommandOptions = new StudiesCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.helpCommandOptions = new HelpCommandOptions();
    }

    public class BaseProjectCommand {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Project identifier", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create new project")
    public class CreateCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-n", "--name"}, description = "Project name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"-a", "--alias"}, description = "Alias", required = true, arity = 1)
        public String alias;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"-o", "--organization"}, description = "Organization", required = false, arity = 1)
        public String organization;

        @Parameter(names = {"--organism-scientific-name"}, description = "Organism scientific name", required = false, arity = 1)
        public String scientificName;

        @Parameter(names = {"--organism-common-name"}, description = "Organism common name", required = false, arity = 1)
        public String commonName;

        @Parameter(names = {"--organism-taxonomy-code"}, description = "Organism taxonomy code", required = false, arity = 1)
        public String taxonomyCode;

        @Parameter(names = {"--organism-assembly"}, description = "Organism assembly", required = false, arity = 1)
        public String assembly;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get project information")
    public class InfoCommandOptions extends BaseProjectCommand {

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;
    }

    @Parameters(commandNames = {"studies"}, commandDescription = "Get all studies from a project")
    public class StudiesCommandOptions extends BaseProjectCommand {

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;
    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update a project")
    public class UpdateCommandOptions extends BaseProjectCommand {

        @Parameter(names = {"-n", "--name"}, description = "Project name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"-o", "--organization"}, description = "Organization", required = false, arity = 1)
        public String organization;

        //@Parameter(names = {"-s", "--status"}, description = "Status", required = false, arity = 1)
        //public String status;

        @Parameter(names = {"-a", "--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;

        @Parameter(names = {"--organism-scientific-name"}, description = "Organism scientific name", required = false, arity = 1)
        public String scientificName;

        @Parameter(names = {"--organism-common-name"}, description = "Organism common name", required = false, arity = 1)
        public String commonName;

        @Parameter(names = {"--organism-taxonomy-code"}, description = "Organism taxonomy code", required = false, arity = 1)
        public String taxonomyCode;

        @Parameter(names = {"--organism-assembly"}, description = "Organism assembly", required = false, arity = 1)
        public String assembly;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete a project")
    public class DeleteCommandOptions extends BaseProjectCommand {

    }

    @Parameters(commandNames = {"help"}, commandDescription = "Help in project")
    public class HelpCommandOptions {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;
    }

}
