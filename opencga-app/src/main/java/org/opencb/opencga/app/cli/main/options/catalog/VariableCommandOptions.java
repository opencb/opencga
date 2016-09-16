package org.opencb.opencga.app.cli.main.options.catalog;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;

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
    public OpencgaCommonCommandOptions commonCommandOptions;

    public VariableCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {
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
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "VariableSet id", required = true, arity = 1)
        public String id;
    }


    @Parameters(commandNames = {"create"}, commandDescription = "Create a variable set.")
    public class CreateCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study-id"}, description = "StudyId", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"-n", "--name"}, description = "Name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"--unique"}, description = "Unique", arity = 0)
        public boolean unique;

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
    public class SearchCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--id"}, description = "Comma separated list of variableSetIds", required = false, arity = 1)
        public String id;

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

        @Parameter(names = {"--json"}, description = "Json file containing the variables to be updated.", required = false)
        public String jsonFile;

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
