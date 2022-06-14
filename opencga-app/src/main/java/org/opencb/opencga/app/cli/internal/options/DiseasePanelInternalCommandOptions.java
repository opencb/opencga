package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.analysis.panel.PanelImportTask;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.internal.InternalCliOptionsParser;
import org.opencb.opencga.core.api.ParamConstants;

@Parameters(commandNames = {"panel"}, commandDescription = "Implement several disease panel tasks")
public class DiseasePanelInternalCommandOptions {

    public final static String OUTDIR_PARAM_NAME = "outdir";

    public final GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    public final InternalCliOptionsParser.JobOptions internalJobOptions;
    public JCommander jCommander;

    public PanelImportCommandOptions panelImportCommandOptions;

    public DiseasePanelInternalCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.internalJobOptions = new InternalCliOptionsParser.JobOptions();
        this.jCommander = jCommander;

        this.panelImportCommandOptions = new PanelImportCommandOptions();
    }

    @Parameters(commandNames = {"import"}, commandDescription = PanelImportTask.DESCRIPTION)
    public class PanelImportCommandOptions {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[user@]project:]study.", arity = 1)
        public String studyId;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outDir;

        @Parameter(names = {"--" + ParamConstants.PANEL_SOURCE_PARAM}, description = ParamConstants.PANEL_IMPORT_SOURCE_DESCRIPTION, required = true,
                arity = 1)
        public String source;

        @Parameter(names = {"--" + ParamConstants.PANEL_SOURCE_ID}, description = ParamConstants.PANEL_SOURCE_ID_DESCRIPTION, arity = 1)
        public String sourceIds;
    }
}
