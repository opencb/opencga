package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.opencga.app.cli.internal.options.JobCommandOptions;

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
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

}
