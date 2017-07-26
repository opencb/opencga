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

package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"variables"}, commandDescription = "Variable set commands")
public class VariableCommandOptions {


    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public SearchCommandOptions searchCommandOptions;

    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public FieldAddCommandOptions fieldAddCommandOptions;
    public FieldDeleteCommandOptions fieldDeleteCommandOptions;
    public FieldRenameCommandOptions fieldRenameCommandOptions;

    public JCommander jCommander;
    public GeneralCliOptions.CommonCommandOptions commonCommandOptions;

    public VariableCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.fieldAddCommandOptions = new FieldAddCommandOptions();
        this.fieldDeleteCommandOptions = new FieldDeleteCommandOptions();
        this.fieldRenameCommandOptions = new FieldRenameCommandOptions();

    }

    public class BaseVariableCommand {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--variable-set"}, description = "VariableSet id or name", required = true, arity = 1)
        public String id;

        @Parameter(names = {"-s", "--study"}, description = "Study [[user@]project:]study where study and project can be either the id or"
                + " alias.", arity = 1)
        public String studyId;
    }


    @Parameters(commandNames = {"create"}, commandDescription = "Create a variable set.")
    public class CreateCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-n", "--name"}, description = "Name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"--unique"}, description = "Unique", arity = 0)
        public boolean unique;

        @Parameter(names = {"--confidential"}, description = "Flag to indicate that the variable set and any annotation set created from"
                + " the current variable set will be confidential and will require of an additional special permission", arity = 0)
        public boolean confidential;

        @Parameter(names = {"--description"}, description = "Description of the variable set", arity = 1)
        public String description;

        @Parameter(names = {"--json"}, description = "Json file containing the variables to be added to the variable set.", required = true)
        public String jsonFile;
    }


    @Parameters(commandNames = {"info"}, commandDescription = "Get variable set information")
    public class InfoCommandOptions extends BaseVariableCommand {

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search for variable sets")
    public class SearchCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"--name"}, description = "name", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;

        @Parameter(names = {"--count"}, description = "Total number of results. PENDING", required = false, arity = 0)
        public boolean count;

    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update variableSet information [PENDING]")
    public class UpdateCommandOptions extends BaseVariableCommand {

        @Parameter(names = {"-n", "--name"}, description = "Name", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--description"}, description = "Description", required = false, arity = 1)
        public String description;

//        @Parameter(names = {"--json"}, description = "Json file containing the variables to be updated.", required = false)
//        public String jsonFile;

    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete an unused variable Set")
    public class DeleteCommandOptions extends BaseVariableCommand {

    }

    @Parameters(commandNames = {"field-add"}, commandDescription = "Add variables to a variable set")
    public class FieldAddCommandOptions extends BaseVariableCommand {

        @Parameter(names = {"--json"}, description = "Json file containing the variables to be added.", required = true)
        public String jsonFile;

    }

    @Parameters(commandNames = {"field-delete"}, commandDescription = "Delete one field from a variable set")
    public class FieldDeleteCommandOptions extends BaseVariableCommand {

        @Parameter(names = {"--name"}, description = "Name.", required = true, arity = 1)
        public String name;
    }

    @Parameters(commandNames = {"field-rename"}, commandDescription = "Rename the field id of a field in a variable set")
    public class FieldRenameCommandOptions extends BaseVariableCommand {

        @Parameter(names = {"--old-name"}, description = "Old Name.", required = true, arity = 1)
        public String oldName;

        @Parameter(names = {"--new-name"}, description = "New Name.", required = true, arity = 1)
        public String newName;
    }


}
