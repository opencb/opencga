package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.sample.SampleIndexTask;
import org.opencb.opencga.app.cli.internal.options.SampleCommandOptions;
import org.opencb.opencga.core.exceptions.ToolException;

import java.nio.file.Path;
import java.nio.file.Paths;

public class SampleCommandExecutor extends InternalCommandExecutor {

    private final SampleCommandOptions sampleCommandOptions;

    public SampleCommandExecutor(SampleCommandOptions options) {
        super(options.sampleCommonOptions);
        sampleCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing sample command line");

        String subCommandString = getParsedSubCommand(sampleCommandOptions.jCommander);
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
        SampleCommandOptions.SecondaryIndex options = sampleCommandOptions.secondaryIndex;

        Path outDir = Paths.get(options.outDir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        // Prepare analysis parameters and config
        SampleIndexTask indexTask = new SampleIndexTask();
        indexTask.setUp(opencgaHome.toString(), new ObjectMap(), outDir, options.commonOptions.token);
        indexTask.start();
    }

}