package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.opencga.analysis.clinical.custom.ZettaInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.team.TeamInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.tiering.CancerTieringInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.tiering.TieringInterpretationAnalysis;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions;

import java.util.List;

import static org.opencb.opencga.analysis.clinical.InterpretationAnalysis.*;
import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.PANEL_DESC;
import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.SAMPLE_ANNOTATION_DESC;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

@Parameters(commandNames = {"clinical"}, commandDescription = "Clinical analysis commands")
public class ClinicalCommandOptions {

    public SearchCommandOptions searchCommandOptions;
    public InfoCommandOptions infoCommandOptions;

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public AclCommandOptions.AclsUpdateCommandOptions aclsUpdateCommandOptions;

    public VariantQueryCommandOptions variantQueryCommandOptions;
    public VariantActionableCommandOptions variantActionableCommandOptions;

    public InterpretationTieringCommandOptions tieringCommandOptions;
    public InterpretationTeamCommandOptions teamCommandOptions;

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

        this.variantQueryCommandOptions = new VariantQueryCommandOptions();
        this.variantActionableCommandOptions = new VariantActionableCommandOptions();

        this.tieringCommandOptions = new InterpretationTieringCommandOptions();
        this.teamCommandOptions = new InterpretationTeamCommandOptions();
    }


    @Parameters(commandNames = {"search"}, commandDescription = "Search clinical analysis")
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
    public class InfoCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--clinical-analysis"}, description = "Clinical analysis", required = true, arity = 1)
        public String clinicalAnalysis;

    }

    @Parameters(commandNames = {"interpretation-query"}, commandDescription = "Filter and fetch interpreted variants")
    public class InterpretationQueryCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.DataModelOptions dataModelOptions = commonDataModelOptions;

    }


    @Parameters(commandNames = {"interpretation-aggregation-stats"}, commandDescription = "Compute aggregation stats over interpreted variants")
    public class InterpretationAggregationStatsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.DataModelOptions dataModelOptions = commonDataModelOptions;

    }

    @Parameters(commandNames = {VariantQueryCommandOptions.VARIANT_QUERY_COMMAND}, commandDescription = "Fetch clinical variants")
    public class VariantQueryCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String VARIANT_QUERY_COMMAND = "variant-query";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public StorageVariantCommandOptions.BasicVariantQueryOptions basicQueryOptions = new StorageVariantCommandOptions.BasicVariantQueryOptions();

        @ParametersDelegate
        public GeneralCliOptions.DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public GeneralCliOptions.NumericOptions numericOptions = commonNumericOptions;

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

        @Parameter(names = {"--stats-missing-allele"}, description = MISSING_ALLELES_DESCR)
        public String missingAlleleCount;

        @Parameter(names = {"--stats-missing-genotype"}, description = MISSING_GENOTYPES_DESCR)
        public String missingGenotypeCount;

        @Parameter(names = {"--annotations", "--output-vcf-info"}, description = "Set variant annotation to return in the INFO column. " +
                "Accepted values include 'all', 'default' or a comma-separated list such as 'gene,biotype,consequenceType'", arity = 1)
        public String annotations;

        @Parameter(names = {"--xref"}, description = ANNOT_XREF_DESCR)
        void setXref(String xref) {
            this.xref = this.xref == null ? xref : this.xref + ',' + xref;
        }
        public String xref;

        @Parameter(names = {"--clinical-significance"}, description = ANNOT_CLINICAL_SIGNIFICANCE_DESCR)
        public String clinicalSignificance;

        @Parameter(names = {"--panel"}, description = PANEL_DESC, arity = 1)
        public String panel;
    }

    @Parameters(commandNames = {VariantActionableCommandOptions.VARIANT_ACTIONABLE_COMMAND}, commandDescription = "Fetch actionable clinical variants")
    public class VariantActionableCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String VARIANT_ACTIONABLE_COMMAND = "variant-actionable";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--" + ParamConstants.SAMPLE_PARAM}, description = ParamConstants.SAMPLE_ID_DESCRIPTION, arity = 1)
        public String sample;

    }


    @Parameters(commandNames = {InterpretationTieringCommandOptions.TIERING_RUN_COMMAND}, commandDescription = TieringInterpretationAnalysis.DESCRIPTION)
    public class InterpretationTieringCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String TIERING_RUN_COMMAND = TieringInterpretationAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--clinical-analysis"}, description = "Clinical analysis", required = true, arity = 1)
        public String clinicalAnalysis;

        @Parameter(names = {"--" + PANELS_PARAM_NAME}, description = "Comma separated list of disease panel IDs", required = true, arity = 1)
        public List<String> panels;

        @Parameter(names = {"--" + PENETRANCE_PARAM_NAME}, description = "Penetrance. Accepted values: COMPLETE, INCOMPLETE", arity = 1)
        public ClinicalProperty.Penetrance penetrance = ClinicalProperty.Penetrance.COMPLETE;

        @Parameter(names = {"--" + INCLUDE_LOW_COVERAGE_PARAM_NAME}, description = "Include low coverage regions", arity = 1)
        public boolean includeLowCoverage;

        @Parameter(names = {"--" + MAX_LOW_COVERAGE_PARAM_NAME}, description = "Maximum low coverage", arity = 1)
        public int maxLowCoverage;

//        @Parameter(names = {"--" + INCLUDE_UNTIERED_VARIANTS_PARAM_NAME}, description = "Reported variants without tier", arity = 1)
//        public boolean includeUntieredVariants;
    }

    @Parameters(commandNames = {InterpretationTeamCommandOptions.TEAM_RUN_COMMAND}, commandDescription = TeamInterpretationAnalysis.DESCRIPTION)
    public class InterpretationTeamCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String TEAM_RUN_COMMAND = TeamInterpretationAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--clinical-analysis"}, description = "Clinical analysis", required = true, arity = 1)
        public String clinicalAnalysis;

        @Parameter(names = {"--" + PANELS_PARAM_NAME}, description = "Comma separated list of disease panel IDs", required = true, arity = 1)
        public List<String> panels;

        @Parameter(names = {"--" + FAMILY_SEGREGATION_PARAM_NAME}, description = "Family segregation", arity = 1)
        public String familySeggregation;

        @Parameter(names = {"--" + INCLUDE_LOW_COVERAGE_PARAM_NAME}, description = "Include low coverage regions", arity = 1)
        public boolean includeLowCoverage;

        @Parameter(names = {"--" + MAX_LOW_COVERAGE_PARAM_NAME}, description = "Maximum low coverage", arity = 1)
        public int maxLowCoverage;

//        @Parameter(names = {"--" + INCLUDE_UNTIERED_VARIANTS_PARAM_NAME}, description = "Reported variants without tier", arity = 1)
//        public boolean includeUntieredVariants;
    }

    @Parameters(commandNames = {InterpretationZettaCommandOptions.ZETTA_RUN_COMMAND}, commandDescription = ZettaInterpretationAnalysis.DESCRIPTION)
    public class InterpretationZettaCommandOptions {

        public static final String ZETTA_RUN_COMMAND = ZettaInterpretationAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--clinical-analysis"}, description = "Clinical analysis", required = true, arity = 1)
        public String clinicalAnalysis;
    }

    @Parameters(commandNames = {InterpretationCancerTieringCommandOptions.CANCER_TIERING_RUN_COMMAND}, commandDescription = CancerTieringInterpretationAnalysis.DESCRIPTION)
    public class InterpretationCancerTieringCommandOptions {

        public static final String CANCER_TIERING_RUN_COMMAND = CancerTieringInterpretationAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--clinical-analysis"}, description = "Clinical analysis", required = true, arity = 1)
        public String clinicalAnalysis;
    }
}
