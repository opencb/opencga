package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.sample.SampleIndexTask;
import org.opencb.opencga.analysis.sample.SampleTsvAnnotationLoader;
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
            case "tsv-load":
                tsvLoad();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void secondaryIndex() throws ToolException {
        SampleCommandOptions.SecondaryIndex options = sampleCommandOptions.secondaryIndex;
        Path outDir = Paths.get(options.outDir);
        toolRunner.execute(SampleIndexTask.class, new ObjectMap(), outDir, options.jobOptions.jobId, options.commonOptions.token);
    }

    private void tsvLoad() throws ToolException {
        SampleCommandOptions.TsvLoad options = sampleCommandOptions.tsvLoad;

        Path outDir = Paths.get(options.outDir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        SampleTsvAnnotationLoader annotationLoader = new SampleTsvAnnotationLoader();
        annotationLoader.setAnnotationSetId(options.annotationSetId);
        annotationLoader.setVariableSetId(options.variableSetId);
        annotationLoader.setPath(options.filePath);
        annotationLoader.setStudy(options.studyId);

        annotationLoader.setUp(opencgaHome.toString(), new ObjectMap(), outDir, options.commonOptions.token);
        annotationLoader.start();
    }
}