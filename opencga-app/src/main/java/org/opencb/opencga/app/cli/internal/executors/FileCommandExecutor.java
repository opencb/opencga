package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.file.*;
import org.opencb.opencga.analysis.variant.operations.VariantIndexOperationTool;
import org.opencb.opencga.app.cli.internal.options.FileCommandOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.FileFetch;
import org.opencb.opencga.core.models.file.PostLinkToolParams;

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
            case "fetch":
                fetch();
                break;
            case "postlink":
                postlink();
                break;
            case "secondary-index":
                secondaryIndex();
                break;
            case "tsv-load":
                tsvLoad();
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
        FileDeleteTask delete = new FileDeleteTask()
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
        FileUnlinkTask unlink = new FileUnlinkTask()
                .setStudy(options.studyId)
                .setFiles(Arrays.asList(org.apache.commons.lang3.StringUtils.split(options.files, ",")));

        unlink.setUp(opencgaHome.toString(), new ObjectMap(), outDir, options.commonOptions.token);
        unlink.start();
    }

    private void postlink() throws ToolException {
        FileCommandOptions.PostlinkCommandOptions options = fileCommandOptions.postlinkCommandOptions;

        Path outDir = Paths.get(options.outDir);

        // Prepare analysis parameters and config
        ObjectMap params = new PostLinkToolParams(
                options.files, options.batchSize)
                .toObjectMap(options.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, options.studyId);

        toolRunner.execute(PostLinkSampleAssociation.class, params, outDir, fileCommandOptions.internalJobOptions.jobId, token);
    }

    private void fetch() throws ToolException {
        FileCommandOptions.FetchCommandOptions options = fileCommandOptions.fetchCommandOptions;

        Path outDir = Paths.get(options.outDir);

        toolRunner.execute(FetchAndRegisterTask.class, new FileFetch(options.url, options.path),
                new ObjectMap(), outDir, null, options.commonOptions.token);
    }

    private void secondaryIndex() throws ToolException {
        FileCommandOptions.SecondaryIndex options = fileCommandOptions.secondaryIndex;
        Path outDir = Paths.get(options.outDir);
        toolRunner.execute(FileIndexTask.class, new ObjectMap(), outDir, options.jobOptions.jobId, options.commonOptions.token);
    }

    private void tsvLoad() throws ToolException {
        FileCommandOptions.TsvLoad options = fileCommandOptions.tsvLoad;

        Path outDir = Paths.get(options.outDir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        FileTsvAnnotationLoader annotationLoader = new FileTsvAnnotationLoader();
        annotationLoader.setAnnotationSetId(options.annotationSetId);
        annotationLoader.setVariableSetId(options.variableSetId);
        annotationLoader.setPath(options.filePath);
        annotationLoader.setStudy(options.studyId);

        annotationLoader.setUp(opencgaHome.toString(), new ObjectMap(), outDir, options.commonOptions.token);
        annotationLoader.start();
    }
}
