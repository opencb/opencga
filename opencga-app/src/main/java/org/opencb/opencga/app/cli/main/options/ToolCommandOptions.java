package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;

/**
 * Created by sgallego on 6/15/16.
 */
@Parameters(commandNames = {"tools"}, commandDescription = "Tools commands")
public class ToolCommandOptions {

    public InfoCommandOptions infoCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public HelpCommandOptions helpCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    public ToolCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.helpCommandOptions = new HelpCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();

    }

    class BaseToolsCommand {
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-id", "--tool-id"}, description = "Tool id", required = true, arity = 1)
        String id;
    }


    @Parameters(commandNames = {"info"}, commandDescription = "Get tool information")
    class InfoCommandOptions extends BaseToolsCommand{
        @Parameter(names = {"--execution"}, description = "execution", required = false, arity = 1)
        String execution;
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search tools")
    class SearchCommandOptions {
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "id", required = false, arity = 1)
        String id;
        @Parameter(names = {"--userId"}, description = "UserId", required = false, arity = 1)
        String userId;
        @Parameter(names = {"--alias"}, description = "alias", required = false, arity = 1)
        String alias;
    }

    @Parameters(commandNames = {"help"}, commandDescription = "Tool help")
    class HelpCommandOptions extends BaseToolsCommand{
        @Parameter(names = {"--execution"}, description = "execution", required = false, arity = 1)
        String execution;
    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update some user attributes")
    class UpdateCommandOptions extends BaseToolsCommand{ }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete a tool")
    class DeleteCommandOptions extends BaseToolsCommand{ }

}
