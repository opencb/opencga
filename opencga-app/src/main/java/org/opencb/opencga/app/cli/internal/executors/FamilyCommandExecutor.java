package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.family.FamilyTsvAnnotationLoader;
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
            case "tsv-load":
                tsvLoad();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void tsvLoad() throws ToolException {
        FamilyCommandOptions.TsvLoad options = familyCommandOptions.tsvLoad;

        Path outDir = Paths.get(options.outDir);

        FamilyTsvAnnotationLoader annotationLoader = new FamilyTsvAnnotationLoader();
        annotationLoader.setAnnotationSetId(options.annotationSetId);
        annotationLoader.setVariableSetId(options.variableSetId);
        annotationLoader.setPath(options.filePath);
        annotationLoader.setStudy(options.studyId);

        annotationLoader.setUp(opencgaHome.toString(), new ObjectMap(), outDir, options.commonOptions.token);
        annotationLoader.start();
    }

}