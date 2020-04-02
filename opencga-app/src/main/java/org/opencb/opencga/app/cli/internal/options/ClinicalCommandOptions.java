package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.opencga.analysis.clinical.tiering.TieringInterpretationAnalysis;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.core.models.common.Enums;

import java.util.List;

import static org.opencb.opencga.analysis.clinical.InterpretationAnalysis.*;

@Parameters(commandNames = {"clinical"}, commandDescription = "Clinical analysis commands")
public class ClinicalCommandOptions {

    public TieringCommandOptions tieringCommandOptions;

    public JCommander jCommander;
    public GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    public GeneralCliOptions.DataModelOptions commonDataModelOptions;
    public GeneralCliOptions.NumericOptions commonNumericOptions;
    public final GeneralCliOptions.JobOptions commonJobOptions;

    public ClinicalCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.commonJobOptions = new GeneralCliOptions.JobOptions();

        this.tieringCommandOptions = new TieringCommandOptions();
    }


    @Parameters(commandNames = {TieringCommandOptions.TIERING_RUN_COMMAND}, commandDescription = TieringInterpretationAnalysis.DESCRIPTION)
    public class TieringCommandOptions {

        public static final String TIERING_RUN_COMMAND = TieringInterpretationAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--" + STUDY_PARAM_NAME}, description = "Study [[user@]project:]study.", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--" + CLINICAL_ANALYISIS_PARAM_NAME}, description = "Clinical Analysis ID", arity = 1)
        public String clinicalAnalysisId;

        @Parameter(names = {"--" + PANELS_PARAM_NAME}, description = "Comma separated list of disease panel IDs", arity = 1)
        public List<String> diseasePanelIds;

        @Parameter(names = {"--" + PENETRANCE_PARAM_NAME}, description = "Penetrance. Accepted values: COMPLETE, INCOMPLETE", arity = 1)
        public ClinicalProperty.Penetrance penetrance = ClinicalProperty.Penetrance.COMPLETE;

        @Parameter(names = {"--" + INCLUDE_LOW_COVERAGE_PARAM_NAME}, description = "Include low coverage regions", arity = 1)
        public boolean includeLowCoverage;

        @Parameter(names = {"--" + MAX_LOW_COVERAGE_PARAM_NAME}, description = "Maximum low coverage", arity = 1)
        public int maxLowCoverage;

        @Parameter(names = {"--" + INCLUDE_UNTIERED_VARIANTS_PARAM_NAME}, description = "Reported variants without tier", arity = 1)
        public boolean includeUntieredVariants;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", required = true, arity = 1)
        public String outdir;
    }

}
