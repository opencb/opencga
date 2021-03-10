package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.cohort.CohortIndexTask;
import org.opencb.opencga.analysis.cohort.CohortTsvAnnotationLoader;
import org.opencb.opencga.app.cli.internal.options.CohortCommandOptions;
import org.opencb.opencga.core.exceptions.ToolException;

import java.nio.file.Path;
import java.nio.file.Paths;

public class CohortCommandExecutor extends InternalCommandExecutor {

    private final CohortCommandOptions cohortCommandOptions;

    public CohortCommandExecutor(CohortCommandOptions options) {
        super(options.cohortCommandOptions);
        cohortCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing cohort command line");

        String subCommandString = getParsedSubCommand(cohortCommandOptions.jCommander);
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

    private void tsvLoad() throws ToolException {
        CohortCommandOptions.TsvLoad options = cohortCommandOptions.tsvLoad;

        Path outDir = Paths.get(options.outDir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        CohortTsvAnnotationLoader annotationLoader = new CohortTsvAnnotationLoader();
        annotationLoader.setAnnotationSetId(options.annotationSetId);
        annotationLoader.setVariableSetId(options.variableSetId);
        annotationLoader.setPath(options.filePath);
        annotationLoader.setStudy(options.studyId);

        annotationLoader.setUp(opencgaHome.toString(), new ObjectMap(), outDir, options.commonOptions.token);
        annotationLoader.start();
    }

    private void secondaryIndex() throws ToolException {
        CohortCommandOptions.SecondaryIndex options = cohortCommandOptions.secondaryIndex;
        Path outDir = Paths.get(options.outDir);
        toolRunner.execute(CohortIndexTask.class, new ObjectMap(), outDir, options.jobOptions.jobId, options.commonOptions.token);
    }

}
