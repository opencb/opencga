package org.opencb.opencga.app.cli.analysis.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;

@Parameters(commandNames = {"tools"}, commandDescription = "Implements different tools for working with tools")
public class ToolsCommandOptions {

    public ListToolCommandOptions listToolCommandOptions;
    public ShowToolCommandOptions showToolCommandOptions;
    public ExecuteToolCommandOptions executeToolCommandOptions;

    public JCommander jCommander;
    public GeneralCliOptions.CommonCommandOptions commonCommandOptions;

    public ToolsCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.listToolCommandOptions = new ListToolCommandOptions();
        this.showToolCommandOptions = new ShowToolCommandOptions();
        this.executeToolCommandOptions = new ExecuteToolCommandOptions();
    }

    @Parameters(commandNames = {"execute"}, commandDescription = "Execute an external tool")
    public class ExecuteToolCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--job"}, description = "Job id containing the information of the job to be executed", required = true,
                arity = 1)
        public String job;
    }

    @Parameters(commandNames = {"list"}, commandDescription = "Print a summary list of all tools")
    public class ListToolCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--filter"}, description = "Some kind of filter", arity = 1)
        public String study;

    }

    @Parameters(commandNames = {"show"}, commandDescription = "Show a summary of the tool")
    public class ShowToolCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--tool-id"}, description = "Full name of the study where the file is classified", arity = 1)
        public String study;

    }

}
