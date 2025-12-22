package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.opencga.analysis.clinical.ClinicalTsvAnnotationLoader;
import org.opencb.opencga.analysis.clinical.exomiser.ExomiserInterpretationAnalysisTool;
import org.opencb.opencga.analysis.clinical.rga.AuxiliarRgaAnalysis;
import org.opencb.opencga.analysis.clinical.rga.RgaAnalysis;
import org.opencb.opencga.analysis.clinical.team.TeamInterpretationAnalysisTool;
import org.opencb.opencga.analysis.clinical.tiering.CancerTieringInterpretationAnalysisTool;
import org.opencb.opencga.analysis.clinical.tiering.TieringInterpretationAnalysisTool;
import org.opencb.opencga.analysis.clinical.zetta.ZettaInterpretationAnalysisTool;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.internal.InternalCliOptionsParser;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.RgaAnalysisParams;
import org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.BasicVariantQueryOptions;

import java.util.List;

import static org.opencb.opencga.analysis.clinical.InterpretationAnalysisTool.*;
import static org.opencb.opencga.core.api.FieldConstants.EXOMISER_CLINICAL_ANALYSIS_DESCRIPTION;
import static org.opencb.opencga.core.api.FieldConstants.EXOMISER_VERSION_DESCRIPTION;
import static org.opencb.opencga.core.models.variant.VariantQueryParams.*;

@Parameters(commandNames = {"clinical"}, commandDescription = "Clinical analysis commands")
public class ClinicalCommandOptions {

    public final TieringCommandOptions tieringCommandOptions;
    public final TeamCommandOptions teamCommandOptions;
    public final ZettaCommandOptions zettaCommandOptions;
    public final CancerTieringCommandOptions cancerTieringCommandOptions;
    public final RgaSecondaryIndexCommandOptions rgaSecondaryIndexCommandOptions;
    public final RgaAuxiliarSecondaryIndexCommandOptions rgaAuxiliarSecondaryIndexCommandOptions;
    public final ExomiserInterpretationCommandOptions exomiserInterpretationCommandOptions;
    public final ImportClinicalAnalysesCommandOptions importClinicalAnalysesCommandOptions;
    public final TsvLoad tsvLoad;

    public JCommander jCommander;
    public GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    public GeneralCliOptions.DataModelOptions commonDataModelOptions;
    public GeneralCliOptions.NumericOptions commonNumericOptions;
    public final InternalCliOptionsParser.JobOptions internalJobOptions;

    public ClinicalCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        internalJobOptions = new InternalCliOptionsParser.JobOptions();
        this.jCommander = jCommander;

        this.tieringCommandOptions = new TieringCommandOptions();
        this.teamCommandOptions = new TeamCommandOptions();
        this.zettaCommandOptions = new ZettaCommandOptions();
        this.cancerTieringCommandOptions = new CancerTieringCommandOptions();
        this.rgaSecondaryIndexCommandOptions = new RgaSecondaryIndexCommandOptions();
        this.rgaAuxiliarSecondaryIndexCommandOptions = new RgaAuxiliarSecondaryIndexCommandOptions();
        this.exomiserInterpretationCommandOptions = new ExomiserInterpretationCommandOptions();
        this.importClinicalAnalysesCommandOptions = new ImportClinicalAnalysesCommandOptions();
        this.tsvLoad = new TsvLoad();
    }

    @Parameters(commandNames = {TieringCommandOptions.TIERING_INTERPRETATION_RUN_COMMAND}, commandDescription =
            TieringInterpretationAnalysisTool.DESCRIPTION)
    public class TieringCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String TIERING_INTERPRETATION_RUN_COMMAND = TieringInterpretationAnalysisTool.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--" + CLINICAL_ANALYISIS_PARAM_NAME}, description = "Clinical analysis", required = true, arity = 1)
        public String clinicalAnalysis;

        @Parameter(names = {"--" + PANELS_PARAM_NAME}, description = "Comma separated list of disease panel IDs", required = true, arity
                = 1)
        public List<String> panels;

        @Parameter(names = {"--" + PENETRANCE_PARAM_NAME}, description = "Penetrance. Accepted values: COMPLETE, INCOMPLETE", arity = 1)
        public ClinicalProperty.Penetrance penetrance = ClinicalProperty.Penetrance.COMPLETE;

        @Parameter(names = {"--" + PRIMARY_INTERPRETATION_PARAM_NAME}, description = "Primary interpretation", arity = 0)
        public boolean primary;

////        @Parameter(names = {"--" + INCLUDE_UNTIERED_VARIANTS_PARAM_NAME}, description = "Reported variants without tier", arity = 1)
////        public boolean includeUntieredVariants;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", required = true, arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {TeamCommandOptions.TEAM_INTERPRETATION_RUN_COMMAND},
            commandDescription = TeamInterpretationAnalysisTool.DESCRIPTION)
    public class TeamCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String TEAM_INTERPRETATION_RUN_COMMAND = TeamInterpretationAnalysisTool.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--" + CLINICAL_ANALYISIS_PARAM_NAME}, description = "Clinical analysis", required = true, arity = 1)
        public String clinicalAnalysis;

        @Parameter(names = {"--" + PANELS_PARAM_NAME}, description = "Comma separated list of disease panel IDs", required = true, arity
                = 1)
        public List<String> panels;

        @Parameter(names = {"--" + FAMILY_SEGREGATION_PARAM_NAME}, description = "Family segregation", arity = 1)
        public String familySeggregation;

        @Parameter(names = {"--" + PRIMARY_INTERPRETATION_PARAM_NAME}, description = "Primary interpretation", arity = 0)
        public boolean primary;

////        @Parameter(names = {"--" + INCLUDE_UNTIERED_VARIANTS_PARAM_NAME}, description = "Reported variants without tier", arity = 1)
////        public boolean includeUntieredVariants;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", required = true, arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {ZettaCommandOptions.ZETTA_INTERPRETATION_RUN_COMMAND},
            commandDescription = ZettaInterpretationAnalysisTool.DESCRIPTION)
    public class ZettaCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String ZETTA_INTERPRETATION_RUN_COMMAND = ZettaInterpretationAnalysisTool.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--" + CLINICAL_ANALYISIS_PARAM_NAME}, description = "Clinical analysis", required = true, arity = 1)
        public String clinicalAnalysis;

        @ParametersDelegate
        public BasicVariantQueryOptions basicQueryOptions = new BasicVariantQueryOptions();

        @Parameter(names = {"--sample"}, description = SAMPLE_DESCR)
        public String samples;

        @Parameter(names = {"--sample-data"}, description = SAMPLE_DATA_DESCR)
        public String sampleData;

        @Parameter(names = {"--sample-annotation"}, description = SAMPLE_ANNOTATION_DESC)
        public String sampleAnnotation;

        @Parameter(names = {"-f", "--file"}, description = FILE_DESCR)
        public String file;

        @Parameter(names = {"--file-data"}, description = FILE_DATA_DESCR)
        public String fileData;

        @Parameter(names = {"--filter"}, description = FILTER_DESCR)
        public String filter;

        @Parameter(names = {"--qual"}, description = QUAL_DESCR)
        public String qual;

        @Parameter(names = {"--score"}, description = SCORE_DESCR)
        public String score;

        @Parameter(names = {"--biotype"}, description = ANNOT_BIOTYPE_DESCR)
        public String geneBiotype;

        @Parameter(names = {"--pmaf", "--population-frequency-maf"}, description = ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR)
        public String populationFreqMaf;

        @Parameter(names = {"--population-frequency-ref"}, description = ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR)
        public String populationFreqRef;

        @Parameter(names = {"--transcript-flag"}, description = ANNOT_TRANSCRIPT_FLAG_DESCR)
        public String flags;

        @Parameter(names = {"--gene-trait-id"}, description = ANNOT_GENE_TRAIT_ID_DESCR)
        public String geneTraitId;

        @Parameter(names = {"--go", "--gene-ontology"}, description = ANNOT_GO_DESCR)
        public String go;

        @Parameter(names = {"--expression"}, description = ANNOT_EXPRESSION_DESCR)
        public String expression;

        @Parameter(names = {"--protein-keywords"}, description = ANNOT_PROTEIN_KEYWORD_DESCR)
        public String proteinKeywords;

        @Parameter(names = {"--drug"}, description = ANNOT_DRUG_DESCR)
        public String drugs;

        @Parameter(names = {"--trait"}, description = ANNOT_TRAIT_DESCR)
        void setTrait(String trait) {
            this.trait = this.trait == null ? trait : this.trait + ',' + trait;
        }

        public String trait;

        @Parameter(names = {"--cohort"}, description = COHORT_DESCR)
        public String cohort;

        @Parameter(names = {"--mgf", "--cohort-stats-mgf"}, description = STATS_MGF_DESCR)
        public String mgf;

        @Parameter(names = {"--cohort-stats-pass"}, description = STATS_PASS_FREQ_DESCR)
        public String cohortStatsPass;

        @Parameter(names = {"--annotations", "--output-vcf-info"}, description = "Set variant annotation to return in the INFO column. " +
                "Accepted values include 'all', 'default' or a comma-separated list such as 'gene,biotype,consequenceType'", arity = 1)
        public String annotations;

        @Parameter(names = {"--xref"}, description = ANNOT_XREF_DESCR)
        public String xref;

        @Parameter(names = {"--clinical"}, description = ANNOT_CLINICAL_DESCR)
        public String clinical;

        @Parameter(names = {"--clinical-significance"}, description = ANNOT_CLINICAL_SIGNIFICANCE_DESCR)
        public String clinicalSignificance;

        @Parameter(names = {"--clinical-confirmed-status"}, description = ANNOT_CLINICAL_CONFIRMED_STATUS_DESCR)
        public boolean clinicalConfirmedStatus;

        @Parameter(names = {"--family"}, description = FAMILY_DESC, arity = 1)
        public String family;

        @Parameter(names = {"--family-disorder"}, description = FAMILY_DISORDER_DESC, arity = 1)
        public String familyPhenotype;

        @Parameter(names = {"--family-segregation"}, description = FAMILY_SEGREGATION_DESCR, arity = 1)
        public String modeOfInheritance;

        @Parameter(names = {"--family-members"}, description = FAMILY_MEMBERS_DESC, arity = 1)
        public String familyMembers;

        @Parameter(names = {"--family-proband"}, description = FAMILY_PROBAND_DESC, arity = 1)
        public String familyProband;

        @Parameter(names = {"--panel"}, description = PANEL_DESC, arity = 1)
        public String panel;
        @Parameter(names = {"--panel-mode-of-inheritance"}, description = PANEL_MOI_DESC, arity = 1)
        public String panelModeOfInheritance;
        @Parameter(names = {"--panel-confidence"}, description = PANEL_CONFIDENCE_DESC, arity = 1)
        public String panelConfidence;
        @Parameter(names = {"--panel-role-in-cancer"}, description = PANEL_ROLE_IN_CANCER_DESC, arity = 1)
        public String panelRoleInCancer;
        @Parameter(names = {"--panel-intersection"}, description = PANEL_INTERSECTION_DESC, arity = 1)
        public Boolean panelIntersection;

        @Parameter(names = {"--" + PRIMARY_INTERPRETATION_PARAM_NAME}, description = "Primary interpretation", arity = 0)
        public boolean primary;

////        @Parameter(names = {"--" + INCLUDE_UNTIERED_VARIANTS_PARAM_NAME}, description = "Reported variants without tier", arity = 1)
////        public boolean includeUntieredVariants;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", arity = 1)
        public String outdir;
    }

    @Parameters(commandNames =  {CancerTieringCommandOptions.CANCER_TIERING_INTERPRETATION_RUN_COMMAND},
            commandDescription = CancerTieringInterpretationAnalysisTool.DESCRIPTION)
    public class CancerTieringCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String CANCER_TIERING_INTERPRETATION_RUN_COMMAND = CancerTieringInterpretationAnalysisTool.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--" + CLINICAL_ANALYISIS_PARAM_NAME}, description = "Clinical analysis", required = true, arity = 1)
        public String clinicalAnalysis;

        @Parameter(names = {"--" + DISCARDED_VARIANTS_PARAM_NAME}, description = "Comma separated list of variant IDs to discard", arity
                = 1)
        public List<String> discardedVariants;

        @Parameter(names = {"--" + PRIMARY_INTERPRETATION_PARAM_NAME}, description = "Primary interpretation", arity = 0)
        public boolean primary;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {RgaSecondaryIndexCommandOptions.RGA_INDEX_RUN_COMMAND}, commandDescription = RgaAnalysis.DESCRIPTION)
    public class RgaSecondaryIndexCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String RGA_INDEX_RUN_COMMAND = RgaAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--" + RgaAnalysisParams.FILE}, description = "Json file containing the KnockoutByIndividual information",
                required = true, arity = 1)
        public String file;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {RgaSecondaryIndexCommandOptions.RGA_INDEX_RUN_COMMAND},
            commandDescription = AuxiliarRgaAnalysis.DESCRIPTION)
    public class RgaAuxiliarSecondaryIndexCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String RGA_AUX_INDEX_RUN_COMMAND = AuxiliarRgaAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {ExomiserInterpretationCommandOptions.EXOMISER_INTERPRETATION_RUN_COMMAND},
            commandDescription = ExomiserInterpretationAnalysisTool.DESCRIPTION)
    public class ExomiserInterpretationCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String EXOMISER_INTERPRETATION_RUN_COMMAND = ExomiserInterpretationAnalysisTool.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--" + CLINICAL_ANALYISIS_PARAM_NAME}, description = EXOMISER_CLINICAL_ANALYSIS_DESCRIPTION,
                required = true, arity = 1)
        public String clinicalAnalysis;

        @Parameter(names = {"--exomiser-version"}, description = EXOMISER_VERSION_DESCRIPTION)
        public String exomiserVersion;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {ImportClinicalAnalysesCommandOptions.IMPORT_COMMAND},
            commandDescription = "Import clinical analyses from a folder")
    public class ImportClinicalAnalysesCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String IMPORT_COMMAND = "import";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input directory where clinical analysis JSON files are located", arity = 1)
        public String input;
    }

    @Parameters(commandNames = {"tsv-load"}, commandDescription = "Load annotations from a TSV file")
    public class TsvLoad extends GeneralCliOptions.StudyOption {

        public static final String TSV_LOAD_COMMAND = ClinicalTsvAnnotationLoader.ID;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--file"}, description = "Path to the TSV file.", required = true, arity = 1)
        public String filePath;

        @Parameter(names = {"--variable-set-id"}, description = ParamConstants.VARIABLE_SET_DESCRIPTION, required = true, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation-set-id"}, description = "AnnotationSet id that will be given to the new annotations.",
                required = true, arity = 1)
        public String annotationSetId;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", required = true, arity = 1)
        public String outDir;
    }
}
