package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.*;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.internal.InternalCliOptionsParser;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.models.common.Enums;

import java.util.HashMap;
import java.util.Map;

@Parameters(commandNames = {"tools"}, commandDescription = "Implements different tools for working with tools")
public class ToolsCommandOptions {

    public ListToolCommandOptions listToolCommandOptions;
    public ShowToolCommandOptions showToolCommandOptions;
    public ExecuteJobCommandOptions executeJobCommandOptions;
    public ExecuteToolCommandOptions executeToolCommandOptions;

    public final InternalCliOptionsParser.JobOptions internalJobOptions;

    public JCommander jCommander;
    public GeneralCliOptions.CommonCommandOptions commonCommandOptions;

    public ToolsCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.internalJobOptions = new InternalCliOptionsParser.JobOptions();
        this.jCommander = jCommander;

        this.listToolCommandOptions = new ListToolCommandOptions();
        this.showToolCommandOptions = new ShowToolCommandOptions();
        this.executeJobCommandOptions = new ExecuteJobCommandOptions();
        this.executeToolCommandOptions = new ExecuteToolCommandOptions();
    }

    @Parameters(commandNames = {"execute-job"}, commandDescription = "Execute a tool given a Catalog Job")
    public class ExecuteJobCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--job"}, description = "Job id containing the information of the job to be executed", required = true,
                arity = 1)
        public String job;
    }

    @Parameters(commandNames = {"execute-tool"}, commandDescription = "Execute a tool")
    public class ExecuteToolCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--tool"}, description = "Tool identifier. It can be either the tool id itself, or the class name.", required = true,
                arity = 1)
        public String toolId;

        @Parameter(names = {"--outdir"}, description = "Output directory", required = true,
                arity = 1)
        public String outDir;

        @DynamicParameter(names = "-P", description = "Tool parameters. -P key=value")
        public Map<String, String> params = new HashMap<>(); //Dynamic parameters must be initialized
    }

    @Parameters(commandNames = {"list"}, commandDescription = "Print a summary list of all tools")
    public class ListToolCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--resource"}, description = "Filter by tool resource", arity = 1)
        public Enums.Resource resource;

        @Parameter(names = {"--type"}, description = "Filter by tool type", arity = 1)
        public Tool.Type type;

    }

    @Parameters(commandNames = {"show"}, commandDescription = "Show a summary of the tool")
    public class ShowToolCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--tool"}, description = "Tool identifier. It can be either the tool id itself, or the class name.", arity = 1)
        public String tool;

    }

}
