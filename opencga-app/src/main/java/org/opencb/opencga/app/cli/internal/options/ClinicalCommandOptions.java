package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.opencga.analysis.clinical.rga.RgaAnalysis;
import org.opencb.opencga.analysis.clinical.team.TeamInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.tiering.CancerTieringInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.tiering.TieringInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.zetta.ZettaInterpretationAnalysis;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.internal.InternalCliOptionsParser;
import org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationTieringCommandOptions;
import org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.BasicVariantQueryOptions;

import java.util.List;

import static org.opencb.opencga.analysis.clinical.InterpretationAnalysis.*;
import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.*;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationCancerTieringCommandOptions.CANCER_TIERING_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationTeamCommandOptions.TEAM_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationZettaCommandOptions.ZETTA_RUN_COMMAND;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

@Parameters(commandNames = {"clinical"}, commandDescription = "Clinical analysis commands")
public class ClinicalCommandOptions {

    public TieringCommandOptions tieringCommandOptions;
    public TeamCommandOptions teamCommandOptions;
    public ZettaCommandOptions zettaCommandOptions;
    public CancerTieringCommandOptions cancerTieringCommandOptions;
    public RgaSecondaryIndexCommandOptions rgaSecondaryIndexCommandOptions;

    public JCommander jCommander;
    public GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    public GeneralCliOptions.DataModelOptions commonDataModelOptions;
    public GeneralCliOptions.NumericOptions commonNumericOptions;
    public final InternalCliOptionsParser.JobOptions internalJobOptions;

    public ClinicalCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.internalJobOptions = new InternalCliOptionsParser.JobOptions();
        this.jCommander = jCommander;


        this.tieringCommandOptions = new TieringCommandOptions();
        this.teamCommandOptions = new TeamCommandOptions();
        this.zettaCommandOptions = new ZettaCommandOptions();
        this.cancerTieringCommandOptions = new CancerTieringCommandOptions();
        this.rgaSecondaryIndexCommandOptions = new RgaSecondaryIndexCommandOptions();
    }

    @Parameters(commandNames = {InterpretationTieringCommandOptions.TIERING_RUN_COMMAND}, commandDescription = TieringInterpretationAnalysis.DESCRIPTION)
    public class TieringCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--" + CLINICAL_ANALYISIS_PARAM_NAME}, description = "Clinical analysis", required = true, arity = 1)
        public String clinicalAnalysis;

        @Parameter(names = {"--" + PANELS_PARAM_NAME}, description = "Comma separated list of disease panel IDs", required = true, arity = 1)
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

    @Parameters(commandNames = {TEAM_RUN_COMMAND}, commandDescription = TeamInterpretationAnalysis.DESCRIPTION)
    public class TeamCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--" + CLINICAL_ANALYISIS_PARAM_NAME}, description = "Clinical analysis", required = true, arity = 1)
        public String clinicalAnalysis;

        @Parameter(names = {"--" + PANELS_PARAM_NAME}, description = "Comma separated list of disease panel IDs", required = true, arity = 1)
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

    @Parameters(commandNames = {ZETTA_RUN_COMMAND}, commandDescription = ZettaInterpretationAnalysis.DESCRIPTION)
    public class ZettaCommandOptions extends GeneralCliOptions.StudyOption {

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

        @Parameter(names = {"--clinical-significance"}, description = ANNOT_CLINICAL_SIGNIFICANCE_DESCR)
        public String clinicalSignificance;

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

        @Parameter(names = {"--" + PRIMARY_INTERPRETATION_PARAM_NAME}, description = "Primary interpretation", arity = 0)
        public boolean primary;

////        @Parameter(names = {"--" + INCLUDE_UNTIERED_VARIANTS_PARAM_NAME}, description = "Reported variants without tier", arity = 1)
////        public boolean includeUntieredVariants;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", required = true, arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {CANCER_TIERING_RUN_COMMAND}, commandDescription = CancerTieringInterpretationAnalysis.DESCRIPTION)
    public class CancerTieringCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--" + CLINICAL_ANALYISIS_PARAM_NAME}, description = "Clinical analysis", required = true, arity = 1)
        public String clinicalAnalysis;

        @Parameter(names = {"--" + DISCARDED_VARIANTS_PARAM_NAME}, description = "Comma separated list of variant IDs to discard", arity = 1)
        public List<String> discardedVariants;

        @Parameter(names = {"--" + PRIMARY_INTERPRETATION_PARAM_NAME}, description = "Primary interpretation", arity = 0)
        public boolean primary;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", required = true, arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {RgaSecondaryIndexCommandOptions.RGA_INDEX_RUN_COMMAND}, commandDescription = RgaAnalysis.DESCRIPTION)
    public class RgaSecondaryIndexCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String RGA_INDEX_RUN_COMMAND = RgaAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"--" + RgaAnalysis.FILE_PARAM}, description = "Json file containing the KnockoutByIndividual information", required = true, arity = 1)
        public String file;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", required = true, arity = 1)
        public String outdir;
    }
}
