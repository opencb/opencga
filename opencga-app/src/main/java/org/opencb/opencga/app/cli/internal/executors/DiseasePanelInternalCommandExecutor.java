package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.panel.PanelImportTask;
import org.opencb.opencga.app.cli.internal.options.DiseasePanelInternalCommandOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.panel.PanelImportParams;

import java.nio.file.Paths;

public class DiseasePanelInternalCommandExecutor extends InternalCommandExecutor {

    private DiseasePanelInternalCommandOptions diseasePanelCommandOptions;
    private String jobId;

    public DiseasePanelInternalCommandExecutor(DiseasePanelInternalCommandOptions diseasePanelCommandOptions) {
        super(diseasePanelCommandOptions.commonCommandOptions);
        this.diseasePanelCommandOptions = diseasePanelCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing disease panel command line");

        String subCommandString = getParsedSubCommand(diseasePanelCommandOptions.jCommander);
        configure();

        jobId = diseasePanelCommandOptions.internalJobOptions.jobId;

        switch (subCommandString) {
            case "import":
                importPanels();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }
    }

    private void importPanels() throws ToolException {
        DiseasePanelInternalCommandOptions.PanelImportCommandOptions options = diseasePanelCommandOptions.panelImportCommandOptions;

        ObjectMap params = new PanelImportParams(options.source, options.sourceIds)
                .toObjectMap(options.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, options.studyId);

        toolRunner.execute(PanelImportTask.class, params, Paths.get(options.outDir), jobId, token);
    }

}
