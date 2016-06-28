package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"variables"}, commandDescription = "Variables commands")
public class VariableCommandOptions {


    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public SearchCommandOptions searchCommandOptions;

    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    // public RenameCommandOptions renameCommandOptions;
    //  public AddCommandOptions addCommandOptions;

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
        //   this.renameCommandOptions = new RenameCommandOptions();
        //   this.addCommandOptions = new AddCommandOptions();

    }

    class BaseVariableCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--variable-id"}, description = "VariableSet id", required = true, arity = 1)
        public Integer id;
    }


    @Parameters(commandNames = {"create"}, commandDescription = "Create sample.")
    class CreateCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study-id"}, description = "StudyId", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"-n", "--name"}, description = "Name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"--unique"}, description = "Unique", required = false, arity = 0)
        public boolean unique;

        @Parameter(names = {"--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--body"}, description = "Variables", required = true)
        public String body;
    }


    @Parameters(commandNames = {"info"}, commandDescription = "Get individual information")
    class InfoCommandOptions extends BaseVariableCommand {
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search for individuals")
    class SearchCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--id"}, description = "CSV list of variableSetIds", required = false, arity = 1)
        public String id;

        @Parameter(names = {"--name"}, description = "name", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;

    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update some user variableSet using GET method [PENDING]")
    class UpdateCommandOptions extends BaseVariableCommand {
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete an unused variable Set")
    class DeleteCommandOptions extends BaseVariableCommand {
    }


}
