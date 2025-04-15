package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.workflow.NextFlowExecutor;
import org.opencb.opencga.app.cli.internal.options.WorkflowCommandOptions;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.externalTool.NextFlowRunParams;

import java.nio.file.Path;
import java.nio.file.Paths;

public class WorkflowCommandExecutor extends InternalCommandExecutor {

    private final WorkflowCommandOptions workflowCommandOptions;

    private String jobId;
    private boolean dryRun;

    public WorkflowCommandExecutor(WorkflowCommandOptions options) {
        super(options.workflowCommonOptions);
        workflowCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing workflow command line");

        jobId = workflowCommandOptions.internalJobOptions.jobId;
        dryRun = workflowCommandOptions.internalJobOptions.dryRun;

        String subCommandString = getParsedSubCommand(workflowCommandOptions.jCommander);
        configure();
        switch (subCommandString) {
            case "workflow":
                nextflowRun();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void nextflowRun() throws ToolException {
        WorkflowCommandOptions.NextflowCommandOptions options = workflowCommandOptions.nextflowCommandOptions;

        Path outDir = Paths.get(options.outDir);

        NextFlowRunParams nextFlowRunParams = new NextFlowRunParams();
        nextFlowRunParams.setId(options.workflowId);

        toolRunner.execute(NextFlowExecutor.class, nextFlowRunParams, QueryOptions.empty(), outDir, jobId, dryRun, token);
    }
}
