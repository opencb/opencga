package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.job.JobIndexTask;
import org.opencb.opencga.app.cli.internal.options.JobCommandOptions;
import org.opencb.opencga.core.exceptions.ToolException;

import java.nio.file.Path;
import java.nio.file.Paths;

public class JobCommandExecutor extends InternalCommandExecutor {

    private final JobCommandOptions jobCommandOptions;

    public JobCommandExecutor(JobCommandOptions options) {
        super(options.jobCommandOptions);
        jobCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing job command line");

        String subCommandString = getParsedSubCommand(jobCommandOptions.jCommander);
        configure();
        switch (subCommandString) {
            case "secondary-index":
                secondaryIndex();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void secondaryIndex() throws ToolException {
        JobCommandOptions.SecondaryIndex options = jobCommandOptions.secondaryIndex;

        Path outDir = Paths.get(options.outDir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        // Prepare analysis parameters and config
        JobIndexTask indexTask = new JobIndexTask();
        indexTask.setUp(opencgaHome.toString(), new ObjectMap(), outDir, options.commonOptions.token);
        indexTask.start();
    }

}
