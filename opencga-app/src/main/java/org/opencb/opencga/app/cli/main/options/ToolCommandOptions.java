package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
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

    public class BaseToolsCommand {
//        @ParametersDelegate
//        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Tool id", required = true, arity = 1)
        public String id;
    }


    @Parameters(commandNames = {"info"}, commandDescription = "Get tool information")
    public class InfoCommandOptions extends BaseToolsCommand {

        @ParametersDelegate
        public OpencgaCliOptionsParser.OpencgaIncludeExcludeCommonCommandOptions commonOptions =
                new OpencgaCliOptionsParser.OpencgaIncludeExcludeCommonCommandOptions();

        @Parameter(names = {"--execution"}, description = "execution", required = false, arity = 1)
        public String execution;
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search tools")
    public class SearchCommandOptions {

        @ParametersDelegate
        public OpencgaCliOptionsParser.OpencgaQueryOptionsCommonCommandOptions commonOptions =
                new OpencgaCliOptionsParser.OpencgaQueryOptionsCommonCommandOptions();

        @Parameter(names = {"--ids"}, description = "Comma separated list of ids", required = false, arity = 1)
        public String id;

        @Parameter(names = {"--userId"}, description = "UserId", required = false, arity = 1)
        public String userId;

        @Parameter(names = {"--alias"}, description = "alias", required = false, arity = 1)
        public String alias;

        @Parameter(names = {"--count"}, description = "Total number of results.", required = false, arity = 0)
        public boolean count;
    }

    @Parameters(commandNames = {"help"}, commandDescription = "Tool help")
    public class HelpCommandOptions extends BaseToolsCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--execution"}, description = "execution", required = false, arity = 1)
        public String execution;
    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update some user attributes")
    public class UpdateCommandOptions extends BaseToolsCommand {
        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete a tool")
    public class DeleteCommandOptions extends BaseToolsCommand {
        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;
    }

}
