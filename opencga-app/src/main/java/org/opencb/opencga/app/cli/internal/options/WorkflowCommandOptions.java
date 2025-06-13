package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.analysis.workflow.NextFlowExecutor;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.internal.InternalCliOptionsParser;

@Parameters(commandNames = {"workflows"}, commandDescription = "Execute workflows")
public class WorkflowCommandOptions {

    public final static String OUTDIR_PARAM_NAME = "outdir";
    public final GeneralCliOptions.CommonCommandOptions workflowCommonOptions;
    public final InternalCliOptionsParser.JobOptions internalJobOptions;
    public JCommander jCommander;

    public final NextflowCommandOptions nextflowCommandOptions;

    public WorkflowCommandOptions(GeneralCliOptions.CommonCommandOptions workflowCommonOptions, JCommander jCommander) {
        this.workflowCommonOptions = workflowCommonOptions;
        this.internalJobOptions = new InternalCliOptionsParser.JobOptions();
        this.jCommander = jCommander;

        this.nextflowCommandOptions = new NextflowCommandOptions();
    }

    @Parameters(commandNames = {NextFlowExecutor.ID}, commandDescription = NextFlowExecutor.DESCRIPTION)
    public class NextflowCommandOptions {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = workflowCommonOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

//        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[organization@]project:]study.",
//        required = true, arity = 1)
//        public String studyId;

        @Parameter(names = {"--workflow-id"}, description = "Workflow id", required = true, arity = 1)
        public String workflowId;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outDir;
    }
}
