package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.templates.TemplateRunner;
import org.opencb.opencga.app.cli.internal.options.StudyCommandOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.study.TemplateParams;

import java.nio.file.Paths;

public class StudyCommandExecutor extends InternalCommandExecutor {

    private final StudyCommandOptions studyCommandOptions;

    public StudyCommandExecutor(StudyCommandOptions options) {
        super(options.commonCommandOptions);
        studyCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing study command line");

        String subCommandString = getParsedSubCommand(studyCommandOptions.jCommander);
        configure();
        switch (subCommandString) {
            case StudyCommandOptions.TemplateLoader.TEMPLATE_RUN_COMMAND:
                templateRun();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void templateRun() throws ToolException {
        StudyCommandOptions.TemplateLoader cliOptions = studyCommandOptions.templateLoader;

        ObjectMap params = new TemplateParams(
                cliOptions.templateId,
                cliOptions.overwrite,
                cliOptions.resume)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.studyId);

        toolRunner.execute(TemplateRunner.class, params, Paths.get(cliOptions.outdir), cliOptions.jobOptions.jobId, token);
    }
}
