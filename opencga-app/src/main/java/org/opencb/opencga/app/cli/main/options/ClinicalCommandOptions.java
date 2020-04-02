package org.opencb.opencga.app.cli.main.options;

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

    public SearchCommandOptions searchCommandOptions;
    public InfoCommandOptions infoCommandOptions;

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public AclCommandOptions.AclsUpdateCommandOptions aclsUpdateCommandOptions;

    public TieringCommandOptions tieringCommandOptions;

    public JCommander jCommander;
    public GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    public GeneralCliOptions.DataModelOptions commonDataModelOptions;
    public GeneralCliOptions.NumericOptions commonNumericOptions;
    public final GeneralCliOptions.JobOptions commonJobOptions;

    public ClinicalCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions,
                                  GeneralCliOptions.DataModelOptions dataModelOptions, GeneralCliOptions.NumericOptions numericOptions,
                                  JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.commonDataModelOptions = dataModelOptions;
        this.commonNumericOptions = numericOptions;
        this.jCommander = jCommander;

        this.commonJobOptions = new GeneralCliOptions.JobOptions();

        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();

        AclCommandOptions aclCommandOptions = new AclCommandOptions(commonCommandOptions);
        this.aclsCommandOptions = aclCommandOptions.getAclsCommandOptions();
        this.aclsUpdateCommandOptions = aclCommandOptions.getAclsUpdateCommandOptions();

        this.tieringCommandOptions = new TieringCommandOptions();
    }


    public class BaseClinicalCommand extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--clinical"}, description = "Clinical analysis id", required = true, arity = 1)
        public String clinical;

    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search clinical analyses")
    public class SearchCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public GeneralCliOptions.NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"--type"}, description = "Clinical analysis type.", arity = 1)
        public String type;

        @Parameter(names = {"--priority"}, description = "Priority.", arity = 1)
        public Enums.Priority priority;

        @Parameter(names = {"--status"}, description = "Status.", arity = 1)
        public String status;

        @Parameter(names = {"--creation-date"}, description = "Creation date.", arity = 1)
        public String creationDate;

        @Parameter(names = {"--modification-date"}, description = "Modification date.", arity = 1)
        public String modificationDate;

        @Parameter(names = {"--due-date"}, description = "Due date", required = false, arity = 1)
        public String dueDate;

        @Parameter(names = {"--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--family"}, description = "Family id", required = false, arity = 1)
        public String family;

        @Parameter(names = {"--proband"}, description = "Proband id of the clinical analysis", required = false, arity = 1)
        public String proband;

        @Parameter(names = {"--sample"}, description = "Proband sample id", arity = 1)
        public String sample;

        @Parameter(names = {"--analyst-assignee"}, description = "Analyst assignee", arity = 1)
        public String assignee;

        @Parameter(names = {"--disorder"}, description = "Disorder id or name", arity = 1)
        public String disorder;

        @Parameter(names = {"--flags"}, description = "Flags", arity = 1)
        public String flags;

    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get clinical analysis information")
    public class InfoCommandOptions extends BaseClinicalCommand {

        @ParametersDelegate
        public GeneralCliOptions.DataModelOptions dataModelOptions = commonDataModelOptions;

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
    }
}
