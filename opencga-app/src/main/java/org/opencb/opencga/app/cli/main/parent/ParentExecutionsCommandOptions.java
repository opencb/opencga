package org.opencb.opencga.app.cli.main.parent;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.core.api.ParamConstants;

public class ParentExecutionsCommandOptions {

    public JCommander jCommander;
    public GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    public TopCommandOptions topCommandOptions;

    public ParentExecutionsCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.jCommander = jCommander;
        this.commonCommandOptions = commonCommandOptions;
        topCommandOptions = new TopCommandOptions();
    }

    @Parameters(commandNames = {"top"}, commandDescription = "Provide a view of executions activity in real time.")
    public class TopCommandOptions {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-d", "--delay"}, description = "Delay between iterations in seconds", arity = 1)
        public int delay = 2;

        @Parameter(names = {"--plain"}, description = "Plain representation, without dependencies.", arity = 0)
        public boolean plain;

        @Parameter(names = {"--columns"}, description = "Output columns to print." +
                " [ID, TOOL_ID, STATUS, EVENTS, STUDY, SUBMISSION, PRIORITY, RUNNING_TIME, START, END, INPUT, OUTPUT, OUTPUT_DIRECTORY]")
        public String columns;

        @Parameter(names = { "--iterations"}, description = "Exit after N iterations", arity = 1)
        public Integer iterations;

        @Parameter(names = {"-n", "--executions"}, description = "Number of executions to print", arity = 1)
        public Integer executionsLimit;

        @Parameter(names = {"--tool-id"}, description = ParamConstants.EXECUTION_TOOL_ID_DESCRIPTION, arity = 1)
        public String toolId;

        @Parameter(names = {"--user-id"}, description = ParamConstants.EXECUTION_USER_DESCRIPTION, arity = 1)
        public String userId;

        @Parameter(names = {"--priority"}, description = ParamConstants.EXECUTION_PRIORITY_DESCRIPTION, arity = 1)
        public String priority;

        @Parameter(names = {"--internal-status"}, description = ParamConstants.EXECUTION_INTERNAL_STATUS_DESCRIPTION, arity = 1)
        public String internalStatus;

        @Parameter(names = {"--study", "-s"}, description = "Study [[user@]project:]study where study and project can be either the ID or" +
                " UUID", required = false, arity = 1)
        public String study;
    }

}
