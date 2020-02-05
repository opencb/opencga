package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.family.FamilyIndexTask;
import org.opencb.opencga.app.cli.internal.options.FamilyCommandOptions;
import org.opencb.opencga.core.exceptions.ToolException;

import java.nio.file.Path;
import java.nio.file.Paths;

public class FamilyCommandExecutor extends InternalCommandExecutor {

    private final FamilyCommandOptions familyCommandOptions;

    public FamilyCommandExecutor(FamilyCommandOptions options) {
        super(options.familyCommandOptions);
        familyCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing family command line");

        String subCommandString = getParsedSubCommand(familyCommandOptions.jCommander);
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
        FamilyCommandOptions.SecondaryIndex options = familyCommandOptions.secondaryIndex;

        Path outDir = Paths.get(options.outDir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        // Prepare analysis parameters and config
        FamilyIndexTask indexTask = new FamilyIndexTask();
        indexTask.setUp(opencgaHome.toString(), new ObjectMap(), outDir, options.commonOptions.token);
        indexTask.start();
    }

}