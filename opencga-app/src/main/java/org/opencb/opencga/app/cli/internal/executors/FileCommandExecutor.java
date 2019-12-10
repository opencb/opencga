package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.file.FileDeleteAction;
import org.opencb.opencga.app.cli.internal.options.FileCommandOptions;
import org.opencb.opencga.core.exception.ToolException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class FileCommandExecutor extends InternalCommandExecutor {

    private final FileCommandOptions fileCommandOptions;

    public FileCommandExecutor(FileCommandOptions options) {
        super(options.fileCommonOptions);
        fileCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing file command line");

        String subCommandString = getParsedSubCommand(fileCommandOptions.jCommander);
        configure();
        switch (subCommandString) {
            case "delete":
                delete();
                break;
            case "unlink":
                unlink();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void delete() throws ToolException {
        FileCommandOptions.DeleteCommandOptions options = fileCommandOptions.deleteCommandOptions;

        Path outDir = Paths.get(options.outDir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        // Prepare analysis parameters and config
        FileDeleteAction delete = new FileDeleteAction()
                .setStudy(options.studyId)
                .setFiles(Arrays.asList(org.apache.commons.lang3.StringUtils.split(options.files, ",")))
                .setSkipTrash(options.skipTrash);

        delete.setUp(opencgaHome.toString(), new ObjectMap(), outDir, options.commonOptions.token);
        delete.start();
    }

    private void unlink() throws ToolException {
        FileCommandOptions.UnlinkCommandOptions options = fileCommandOptions.unlinkCommandOptions;

        Path outDir = Paths.get(options.outDir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        // Prepare analysis parameters and config
        FileDeleteAction delete = new FileDeleteAction()
                .setStudy(options.studyId)
                .setFiles(Arrays.asList(org.apache.commons.lang3.StringUtils.split(options.files, ",")))
                .setUnlink(true);

        delete.setUp(opencgaHome.toString(), new ObjectMap(), outDir, options.commonOptions.token);
        delete.start();
    }
}
