package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.individual.IndividualIndexTask;
import org.opencb.opencga.app.cli.internal.options.IndividualCommandOptions;
import org.opencb.opencga.core.exceptions.ToolException;

import java.nio.file.Path;
import java.nio.file.Paths;

public class IndividualCommandExecutor extends InternalCommandExecutor {

    private final IndividualCommandOptions individualCommandOptions;

    public IndividualCommandExecutor(IndividualCommandOptions options) {
        super(options.individualCommandOptions);
        individualCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing individual command line");

        String subCommandString = getParsedSubCommand(individualCommandOptions.jCommander);
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
        IndividualCommandOptions.SecondaryIndex options = individualCommandOptions.secondaryIndex;

        Path outDir = Paths.get(options.outDir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        // Prepare analysis parameters and config
        IndividualIndexTask indexTask = new IndividualIndexTask();
        indexTask.setUp(opencgaHome.toString(), new ObjectMap(), outDir, options.commonOptions.token);
        indexTask.start();
    }

}