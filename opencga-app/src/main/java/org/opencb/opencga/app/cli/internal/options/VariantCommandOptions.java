/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.*;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.opencga.analysis.family.qc.FamilyQcAnalysis;
import org.opencb.opencga.analysis.individual.qc.IndividualQcAnalysis;
import org.opencb.opencga.analysis.sample.qc.SampleQcAnalysis;
import org.opencb.opencga.analysis.variant.VariantExportTool;
import org.opencb.opencga.analysis.variant.gwas.GwasAnalysis;
import org.opencb.opencga.analysis.variant.inferredSex.InferredSexAnalysis;
import org.opencb.opencga.analysis.variant.julie.JulieTool;
import org.opencb.opencga.analysis.variant.knockout.KnockoutAnalysis;
import org.opencb.opencga.analysis.variant.mendelianError.MendelianErrorAnalysis;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis;
import org.opencb.opencga.analysis.variant.operations.VariantFamilyIndexOperationTool;
import org.opencb.opencga.analysis.variant.operations.VariantIndexOperationTool;
import org.opencb.opencga.analysis.variant.operations.VariantSampleIndexOperationTool;
import org.opencb.opencga.analysis.variant.relatedness.RelatednessAnalysis;
import org.opencb.opencga.analysis.variant.samples.SampleEligibilityAnalysis;
import org.opencb.opencga.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.opencga.analysis.wrappers.PlinkWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.gatk.GatkWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.rvtests.RvtestsWrapperAnalysis;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.DataModelOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.NumericOptions;
import org.opencb.opencga.app.cli.internal.InternalCliOptionsParser;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.variant.AnnotationVariantQueryParams;
import org.opencb.opencga.core.models.variant.SampleVariantFilterParams;
import org.opencb.opencga.core.tools.variant.IndividualQcAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.*;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.CohortVariantStatsCommandOptions.COHORT_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.FamilyIndexCommandOptions.FAMILY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.FamilyIndexCommandOptions.FAMILY_INDEX_COMMAND_DESCRIPTION;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleIndexCommandOptions.SAMPLE_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleIndexCommandOptions.SAMPLE_INDEX_COMMAND_DESCRIPTION;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleVariantStatsCommandOptions.SAMPLE_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSampleQueryCommandOptions.SAMPLE_QUERY_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSecondaryIndexCommandOptions.SECONDARY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSecondaryIndexDeleteCommandOptions.SECONDARY_INDEX_DELETE_COMMAND;
import static org.opencb.opencga.core.api.ParamConstants.*;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.AggregateCommandOptions.AGGREGATE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.AggregateCommandOptions.AGGREGATE_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.AggregateFamilyCommandOptions.AGGREGATE_FAMILY_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.AggregateFamilyCommandOptions.AGGREGATE_FAMILY_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.*;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationDeleteCommandOptions.ANNOTATION_DELETE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationDeleteCommandOptions.ANNOTATION_DELETE_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationMetadataCommandOptions.ANNOTATION_METADATA_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationMetadataCommandOptions.ANNOTATION_METADATA_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationQueryCommandOptions.ANNOTATION_QUERY_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationQueryCommandOptions.ANNOTATION_QUERY_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationSaveCommandOptions.ANNOTATION_SAVE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationSaveCommandOptions.ANNOTATION_SAVE_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.VariantDeleteCommandOptions.VARIANT_DELETE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.VariantDeleteCommandOptions.VARIANT_DELETE_COMMAND_DESCRIPTION;

/**
 * Created by pfurio on 23/11/16.
 */
@Parameters(commandNames = {"variant"}, commandDescription = "Variant commands")
public class VariantCommandOptions {

    public final VariantIndexCommandOptions indexVariantCommandOptions;
    public final VariantDeleteCommandOptions variantDeleteCommandOptions;
    public final VariantSecondaryIndexCommandOptions variantSecondaryIndexCommandOptions;
    public final VariantSecondaryIndexDeleteCommandOptions variantSecondaryIndexDeleteCommandOptions;
    public final VariantQueryCommandOptions queryVariantCommandOptions;
    public final VariantExportCommandOptions exportVariantCommandOptions;
    public final VariantStatsCommandOptions statsVariantCommandOptions;
    public final VariantScoreIndexCommandOptions variantScoreIndexCommandOptions;
    public final VariantScoreDeleteCommandOptions variantScoreDeleteCommandOptions;
    public final SampleIndexCommandOptions sampleIndexCommandOptions;
    public final FamilyIndexCommandOptions familyIndexCommandOptions;
    public final VariantAnnotateCommandOptions annotateVariantCommandOptions;
    public final AnnotationSaveCommandOptions annotationSaveSnapshotCommandOptions;
    public final AnnotationDeleteCommandOptions annotationDeleteCommandOptions;
    public final AnnotationQueryCommandOptions annotationQueryCommandOptions;
    public final AnnotationMetadataCommandOptions annotationMetadataCommandOptions;
    public final AggregateFamilyCommandOptions fillGapsVariantCommandOptions;
    public final AggregateCommandOptions aggregateCommandOptions;
    public final VariantExportStatsCommandOptions exportVariantStatsCommandOptions;
    public final VariantImportCommandOptions importVariantCommandOptions;
    public final VariantIbsCommandOptions ibsVariantCommandOptions;
    public final VariantSampleQueryCommandOptions sampleQueryCommandOptions;
    public final VariantSamplesFilterCommandOptions samplesFilterCommandOptions;
    public final VariantHistogramCommandOptions histogramCommandOptions;
    public final JulieRunCommandOptions julieRunCommandOptions;

    // Analysis
    public final GwasCommandOptions gwasCommandOptions;
    public final SampleVariantStatsCommandOptions sampleVariantStatsCommandOptions;
    public final SampleVariantStatsQueryCommandOptions sampleVariantStatsQueryCommandOptions;
    public final CohortVariantStatsCommandOptions cohortVariantStatsCommandOptions;
    public final CohortVariantStatsQueryCommandOptions cohortVariantStatsQueryCommandOptions;
    public final KnockoutCommandOptions knockoutCommandOptions;
    public final SampleEligibilityCommandOptions sampleEligibilityCommandOptions;
    public final MutationalSignatureCommandOptions mutationalSignatureCommandOptions;
    public final MendelianErrorCommandOptions mendelianErrorCommandOptions;
    public final InferredSexCommandOptions inferredSexCommandOptions;
    public final RelatednessCommandOptions relatednessCommandOptions;
    public final FamilyQcCommandOptions familyQcCommandOptions;
    public final IndividualQcCommandOptions individualQcCommandOptions;
    public final SampleQcCommandOptions sampleQcCommandOptions;

    // Wrappers
    public final PlinkCommandOptions plinkCommandOptions;
    public final RvtestsCommandOptions rvtestsCommandOptions;
    public final GatkCommandOptions gatkCommandOptions;

    public final JCommander jCommander;
    public final GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    public final DataModelOptions commonDataModelOptions;
    public final NumericOptions commonNumericOptions;
    public final GeneralCliOptions.JobOptions commonJobOptions;
    public final InternalCliOptionsParser.JobOptions internalJobOptions;
    private final Object commonJobOptionsObject;
    private final Object internalJobOptionsObject;

    public VariantCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, DataModelOptions dataModelOptions,
                                 NumericOptions numericOptions, JCommander jCommander, boolean withFullJobParams) {
        this.commonCommandOptions = commonCommandOptions;
        this.commonDataModelOptions = dataModelOptions;
        this.commonNumericOptions = numericOptions;
        this.commonJobOptions = new GeneralCliOptions.JobOptions();
        this.internalJobOptions = new InternalCliOptionsParser.JobOptions();
        this.commonJobOptionsObject = withFullJobParams ? commonJobOptions : new Object();
        this.internalJobOptionsObject = withFullJobParams ? new Object() : internalJobOptions;
        this.jCommander = jCommander;

        this.indexVariantCommandOptions = new VariantIndexCommandOptions();
        this.variantDeleteCommandOptions = new VariantDeleteCommandOptions();
        this.variantSecondaryIndexCommandOptions = new VariantSecondaryIndexCommandOptions();
        this.variantSecondaryIndexDeleteCommandOptions = new VariantSecondaryIndexDeleteCommandOptions();
        this.queryVariantCommandOptions = new VariantQueryCommandOptions();
        this.exportVariantCommandOptions = new VariantExportCommandOptions();
        this.statsVariantCommandOptions = new VariantStatsCommandOptions();
        this.variantScoreIndexCommandOptions = new VariantScoreIndexCommandOptions();
        this.variantScoreDeleteCommandOptions = new VariantScoreDeleteCommandOptions();
        this.sampleIndexCommandOptions = new SampleIndexCommandOptions();
        this.familyIndexCommandOptions = new FamilyIndexCommandOptions();
        this.annotateVariantCommandOptions = new VariantAnnotateCommandOptions();
        this.annotationSaveSnapshotCommandOptions = new AnnotationSaveCommandOptions();
        this.annotationDeleteCommandOptions = new AnnotationDeleteCommandOptions();
        this.annotationQueryCommandOptions = new AnnotationQueryCommandOptions();
        this.annotationMetadataCommandOptions = new AnnotationMetadataCommandOptions();
        this.fillGapsVariantCommandOptions = new AggregateFamilyCommandOptions();
        this.aggregateCommandOptions = new AggregateCommandOptions();
        this.exportVariantStatsCommandOptions = new VariantExportStatsCommandOptions();
        this.importVariantCommandOptions = new VariantImportCommandOptions();
        this.ibsVariantCommandOptions = new VariantIbsCommandOptions();
        this.sampleQueryCommandOptions = new VariantSampleQueryCommandOptions();
        this.samplesFilterCommandOptions = new VariantSamplesFilterCommandOptions();
        this.histogramCommandOptions = new VariantHistogramCommandOptions();
        this.julieRunCommandOptions = new JulieRunCommandOptions();
        this.gwasCommandOptions = new GwasCommandOptions();
        this.sampleVariantStatsCommandOptions = new SampleVariantStatsCommandOptions();
        this.sampleVariantStatsQueryCommandOptions = new SampleVariantStatsQueryCommandOptions();
        this.cohortVariantStatsCommandOptions = new CohortVariantStatsCommandOptions();
        this.cohortVariantStatsQueryCommandOptions = new CohortVariantStatsQueryCommandOptions();
        this.knockoutCommandOptions = new KnockoutCommandOptions();
        this.sampleEligibilityCommandOptions = new SampleEligibilityCommandOptions();
        this.mutationalSignatureCommandOptions = new MutationalSignatureCommandOptions();
        this.mendelianErrorCommandOptions = new MendelianErrorCommandOptions();
        this.inferredSexCommandOptions = new InferredSexCommandOptions();
        this.relatednessCommandOptions = new RelatednessCommandOptions();
        this.familyQcCommandOptions = new FamilyQcCommandOptions();
        this.individualQcCommandOptions = new IndividualQcCommandOptions();
        this.sampleQcCommandOptions = new SampleQcCommandOptions();
        this.plinkCommandOptions = new PlinkCommandOptions();
        this.rvtestsCommandOptions = new RvtestsCommandOptions();
        this.gatkCommandOptions = new GatkCommandOptions();
    }

    @Parameters(commandNames = {VariantIndexCommandOptions.INDEX_RUN_COMMAND}, commandDescription = VariantIndexOperationTool.DESCRIPTION)
    public class VariantIndexCommandOptions extends GeneralCliOptions.StudyOption {
        public static final String INDEX_RUN_COMMAND = "index-run";

        @ParametersDelegate
        public GenericVariantIndexOptions genericVariantIndexOptions = new GenericVariantIndexOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--file"}, description = "List of files to be indexed.", required = true, arity = 1)
        public String fileId = null;

//        @Parameter(names = {"--transformed-files"}, description = "CSV of paths corresponding to the location of the transformed files.",
//                arity = 1)
//        public String transformedPaths = null;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = false, arity = 1)
        public String outdir = null;

        @Parameter(names = {"--stdin"}, description = "Read the variants file from the standard input")
        public boolean stdin;

        @Parameter(names = {"--stdout"}, description = "Write the transformed variants file to the standard output")
        public boolean stdout;

        @Parameter(names = {"--skip-indexed-files"}, description = "Do not fail if any of the input files was already indexed.")
        public boolean skipIndexedFiles;
    }

    @Parameters(commandNames = {SECONDARY_INDEX_COMMAND}, commandDescription = "Creates a secondary index using a search engine")
    public class VariantSecondaryIndexCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String SECONDARY_INDEX_COMMAND = "secondary-index";
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-p", "--project"}, description = "Project to index.", arity = 1)
        public String project;

        @Parameter(names = {"-r", "--region"}, description = VariantQueryParam.REGION_DESCR)
        public String region;

        @Parameter(names = {"--sample"}, description = "Samples to index."
                + " If provided, all sample data will be added to the secondary index.", arity = 1)
        public List<String> sample;

        @Parameter(names = {"--overwrite"}, description = "Overwrite search index for all files and variants. Repeat operation for already processed variants.")
        public boolean overwrite;

        @Parameter(names = {"--outdir"}, description = "Output directory", hidden = true)
        public String outdir;
    }

    @Parameters(commandNames = {SECONDARY_INDEX_DELETE_COMMAND}, commandDescription = "Remove a secondary index from the search engine")
    public class VariantSecondaryIndexDeleteCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String SECONDARY_INDEX_DELETE_COMMAND = "secondary-index-delete";
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--sample"}, description = "Samples to remove. Needs to provide all the samples in the secondary index.",
                required = true, arity = 1)
        public String sample;

        // TODO Use this outdir to store the operation-status.json
        @Parameter(names = {"--outdir"}, description = "[PENDING]", hidden = true)
        public String outdir;
    }

    @Parameters(commandNames = {VARIANT_DELETE_COMMAND}, commandDescription = VARIANT_DELETE_COMMAND_DESCRIPTION)
    public class VariantDeleteCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GenericVariantDeleteOptions genericVariantDeleteOptions = new GenericVariantDeleteOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", arity = 1)
        public String outdir = null;
    }

    @Parameters(commandNames = {"query"}, commandDescription = ParamConstants.VARIANTS_QUERY_DESCRIPTION)
    public class VariantQueryCommandOptions extends AbstractVariantQueryCommandOptions {

        // FIXME: This param should not be in the INTERNAL command line!
        @Parameter(names = {"--mode"}, description = "Communication mode. grpc|rest|auto.")
        public String mode = "auto";

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory. [STDOUT]", arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {VariantExportCommandOptions.EXPORT_RUN_COMMAND}, commandDescription = VariantExportTool.DESCRIPTION)
    public class VariantExportCommandOptions extends AbstractVariantQueryCommandOptions {
        public static final String EXPORT_RUN_COMMAND = "export-run";

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1)
        public String outdir;
    }

    public class AbstractVariantQueryCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GenericVariantQueryOptions genericVariantQueryOptions = new GenericVariantQueryOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--output-file-name"}, description = "Output file name.", arity = 1)
        public String outputFileName;

        @Parameter(names = {"--compress", "-z"}, description = "Compress output file.", arity = 0)
        public boolean compress;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"--sample-annotation"}, description = SAMPLE_ANNOTATION_DESC)
        public String sampleAnnotation;

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;

        @Parameter(names = {"--family"}, description = FAMILY_DESC, arity = 1)
        public String family;

        @Parameter(names = {"--family-disorder"}, description = FAMILY_DISORDER_DESC, arity = 1)
        public String familyPhenotype;

        @Parameter(names = {"--family-segregation"}, description = FAMILY_SEGREGATION_DESCR, arity = 1)
        public String familySegregation;

        @Parameter(names = {"--family-members"}, description = FAMILY_MEMBERS_DESC, arity = 1)
        public String familyMembers;

        @Parameter(names = {"--family-proband"}, description = FAMILY_PROBAND_DESC, arity = 1)
        public String familyProband;

        @Parameter(names = {"--panel"}, description = PANEL_DESC, arity = 1)
        public String panel;

        @Parameter(names = {"--saved-filter"}, description = SAVED_FILTER_DESCR, arity = 1)
        public String savedFilter;

        // FIXME: This param should not be in the REST command line!
        @Parameter(names = {"--variants-file"}, description = "GFF File with regions")
        public String variantsFile;
    }

    @Parameters(commandNames = {VariantStatsCommandOptions.STATS_RUN_COMMAND}, commandDescription = VariantStatsAnalysis.DESCRIPTION)
    public class VariantStatsCommandOptions extends GeneralCliOptions.StudyOption {
        public static final String STATS_RUN_COMMAND = "stats-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--cohort"}, description = "Cohort Ids for the cohorts to be calculated.")
        public List<String> cohort;

        @Parameter(names = {"--cohort-ids"}, hidden = true)
        public void setCohortIds(List<String> cohortIds) {
            this.cohort = cohortIds;
        }

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = false, arity = 1)
        public String outdir = null;

        @Parameter(names = {"--samples"}, description = "List of samples to use as cohort to calculate stats")
        public List<String> samples;

        @Parameter(names = {"--region"}, description = "Region to calculate.")
        public String region;

        @Parameter(names = {"--gene"}, description = "List of genes.")
        public String gene;

        @Parameter(names = {"--output-file-name"}, description = "Output file name. Default: database name", arity = 1)
        public String fileName;

        @Parameter(names = {"--aggregated"}, description = "Select the type of aggregated VCF file: none, basic, EVS or ExAC", arity = 1)
        public Aggregation aggregated = Aggregation.NONE;

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF file")
        public String aggregationMappingFile;
    }

    @Parameters(commandNames = {GenericVariantScoreIndexCommandOptions.SCORE_INDEX_COMMAND}, commandDescription = GenericVariantScoreIndexCommandOptions.SCORE_INDEX_COMMAND_DESCRIPTION)
    public class VariantScoreIndexCommandOptions extends GenericVariantScoreIndexCommandOptions {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.StudyOption study = new GeneralCliOptions.StudyOption();

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = false, arity = 1)
        public String outdir = null;
    }

    @Parameters(commandNames = {GenericVariantScoreDeleteCommandOptions.SCORE_DELETE_COMMAND}, commandDescription = GenericVariantScoreDeleteCommandOptions.SCORE_DELETE_COMMAND_DESCRIPTION)
    public class VariantScoreDeleteCommandOptions extends GenericVariantScoreDeleteCommandOptions {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.StudyOption study = new GeneralCliOptions.StudyOption();

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = false, arity = 1)
        public String outdir = null;
    }

    @Parameters(commandNames = {SAMPLE_INDEX_COMMAND}, commandDescription = SAMPLE_INDEX_COMMAND_DESCRIPTION)
    public class SampleIndexCommandOptions extends GeneralCliOptions.StudyOption {
        public static final String SAMPLE_INDEX_COMMAND = "sample-index";
        public static final String SAMPLE_INDEX_COMMAND_DESCRIPTION = VariantSampleIndexOperationTool.DESCRIPTION;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--sample"}, required = true, description = "Samples to include in the index. " +
                "Use \"" + VariantQueryUtils.ALL + "\" to annotate the index for all samples in the study.")
        public List<String> sample;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = false, arity = 1)
        public String outdir;

        @Parameter(names = {"--build-index"}, description = "Build sample index.", arity = 0)
        public boolean buildIndex;

        @Parameter(names = {"--annotate"}, description = "Annotate sample index", arity = 0)
        public boolean annotate;

        @Parameter(names = {"--overwrite"}, description = "Overwrite existing values", arity = 0)
        public boolean overwrite;
    }

    @Parameters(commandNames = {FAMILY_INDEX_COMMAND}, commandDescription = FAMILY_INDEX_COMMAND_DESCRIPTION)
    public class FamilyIndexCommandOptions extends GeneralCliOptions.StudyOption {
        public static final String FAMILY_INDEX_COMMAND = "family-index";
        public static final String FAMILY_INDEX_COMMAND_DESCRIPTION = VariantFamilyIndexOperationTool.DESCRIPTION;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--family"}, required = true, description = "Families to index. " +
                "Use \"" + VariantQueryUtils.ALL + "\" to index all families in the study.")
        public List<String> family;

        @Parameter(names = {"--overwrite"}, description = "Overwrite existing values")
        public boolean overwrite = false;

        @Parameter(names = {"--skip-incomplete-families"}, description = "Do not process incomplete families.")
        public boolean skipIncompleteFamilies = false;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = false, arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {VariantAnnotateCommandOptions.ANNOTATION_INDEX_COMMAND}, commandDescription = GenericVariantAnnotateOptions.ANNOTATE_DESCRIPTION)
    public class VariantAnnotateCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String ANNOTATION_INDEX_COMMAND = "annotation-index";
        @ParametersDelegate
        public GenericVariantAnnotateOptions genericVariantAnnotateOptions = new GenericVariantAnnotateOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-p", "--project"}, description = "Project to annotate.", arity = 1)
        public String project;

//        @Parameter(names = {"-s", "--study-id"}, description = "Studies to annotate. Must be in the same database.", arity = 1)
//        public String study;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", required = true, arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {ANNOTATION_SAVE_COMMAND}, commandDescription = ANNOTATION_SAVE_COMMAND_DESCRIPTION)
    public class AnnotationSaveCommandOptions extends GenericAnnotationSaveCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = false, arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {ANNOTATION_DELETE_COMMAND}, commandDescription = ANNOTATION_DELETE_COMMAND_DESCRIPTION)
    public class AnnotationDeleteCommandOptions extends GenericAnnotationDeleteCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = false, arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {ANNOTATION_QUERY_COMMAND}, commandDescription = ANNOTATION_QUERY_COMMAND_DESCRIPTION)
    public class AnnotationQueryCommandOptions extends GenericAnnotationQueryCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.DataModelOptions dataModelOptions = new DataModelOptions();

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;

        @Parameter(names = {"--skip"}, description = "Skip some number of elements.", required = false, arity = 1)
        public int skip;

        @Parameter(names = {"--limit"}, description = "Limit the number of returned elements.", required = false, arity = 1)
        public int limit;
    }

    @Parameters(commandNames = {ANNOTATION_METADATA_COMMAND}, commandDescription = ANNOTATION_METADATA_COMMAND_DESCRIPTION)
    public class AnnotationMetadataCommandOptions extends GenericAnnotationMetadataCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;
    }

    @Parameters(commandNames = {AGGREGATE_FAMILY_COMMAND}, commandDescription = AGGREGATE_FAMILY_COMMAND_DESCRIPTION)
    public class AggregateFamilyCommandOptions extends GeneralCliOptions.StudyOption {

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = true, arity = 1)
        public String outdir;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GenericAggregateFamilyOptions genericAggregateFamilyOptions = new GenericAggregateFamilyOptions();

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;
    }

    @Parameters(commandNames = {AGGREGATE_COMMAND}, commandDescription = AGGREGATE_COMMAND_DESCRIPTION)
    public class AggregateCommandOptions extends GeneralCliOptions.StudyOption {

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = true, arity = 1)
        public String outdir;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GenericAggregateCommandOptions aggregateCommandOptions = new GenericAggregateCommandOptions();

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

    }

    @Parameters(commandNames = {VariantExportStatsCommandOptions.STATS_EXPORT_RUN_COMMAND}, commandDescription = "Export calculated variant stats and frequencies")
    public class VariantExportStatsCommandOptions {
        public static final String STATS_EXPORT_RUN_COMMAND = "stats-export-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

//        @ParametersDelegate
//        public QueryCommandOptions queryOptions = new QueryCommandOptions();

//        @Parameter(names = {"--of", "--output-format"}, description = "Output format: vcf, vcf.gz, tsv, tsv.gz, cellbase, cellbase.gz, json or json.gz", arity = 1)
//        public String outputFormat = "tsv";

        @Parameter(names = {"-r", "--region"}, description = "CSV list of regions: {chr}[:{start}-{end}]. example: 2,3:1000000-2000000",
                required = false)
        public String region;

        @Parameter(names = {"--region-file"}, description = "GFF File with regions")
        public String regionFile;

        @Parameter(names = {"-g", "--gene"}, description = "CSV list of genes")
        public String gene;

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;

        @Parameter(names = {"-s", "--study"}, description = "A comma separated list of studies to be returned")
        public String study;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory. [STDOUT]", arity = 1)
        public String outdir;

        @Parameter(names = {"--output-file-name"}, description = "Output file name.", arity = 1)
        public String outputFileName;
    }

    @Parameters(commandNames = {"import"}, commandDescription = "Import a variants dataset into an empty study")
    public class VariantImportCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-s", "--study"}, description = "Study where to load the variants", required = true)
        public String study;

        @Parameter(names = {"-i", "--input"}, description = "Variants input file in avro format", required = true)
        public String input;

    }

    @Parameters(commandNames = {"ibs"}, commandDescription = "[EXPERIMENTAL] Identity By State Clustering")
    public class VariantIbsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--sample"}, description = "List of samples to check. By default, all samples")
        public String samples;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.")
        public String outdir = "-";
    }

    @Parameters(commandNames = {VariantSamplesFilterCommandOptions.SAMPLE_RUN_COMMAND}, commandDescription = "Get samples given a set of variants")
    public class VariantSamplesFilterCommandOptions extends GeneralCliOptions.StudyOption {
        public static final String SAMPLE_RUN_COMMAND = "sample-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public SampleVariantFilterParamsAnnotated toolParams = new SampleVariantFilterParamsAnnotated();

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.")
        public String outdir;
    }

    @Parameters(commandNames = {SAMPLE_QUERY_COMMAND}, commandDescription = "Get sample data of a given variant")
    public class VariantSampleQueryCommandOptions {
        public static final String SAMPLE_QUERY_COMMAND = "sample-query";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--variant"}, description = "Variant to query", required = true)
        public String variant;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--genotype"}, description = "Genotypes that the sample must have to be selected")
        public List<String> genotype;

    }

    public static class SampleVariantFilterParamsAnnotated extends SampleVariantFilterParams {

        //TODO
//        @Parameter(names = {"--sample-filter"}, description = SAMPLE_FILTER_DESC)
//        public String sampleFilter;

        @Parameter(names = {"--genotypes"}, description = "Genotypes that the sample must have to be selected")
        @Override
        public SampleVariantFilterParams setGenotypes(List<String> genotypes) {
            return super.setGenotypes(genotypes);
        }

        @Parameter(names = {"--sample"}, description = "List of samples to check. By default, all samples")
        @Override
        public SampleVariantFilterParams setSample(List<String> sample) {
            return super.setSample(sample);
        }

        @Parameter(names = {"--samples-in-all-variants"}, description = "Samples must be present in ALL variants instead of ANY variant.")
        @Override
        public SampleVariantFilterParams setSamplesInAllVariants(boolean samplesInAllVariants) {
            return super.setSamplesInAllVariants(samplesInAllVariants);
        }

        @Parameter(names = {"--max-variants"}, description = "Limit the maximum number of variants to look up")
        @Override
        public SampleVariantFilterParams setMaxVariants(int maxVariants) {
            return super.setMaxVariants(maxVariants);
        }

        @Parameter(names = {"--id"}, description = VariantQueryParam.ID_DESCR)
        @Override
        public AnnotationVariantQueryParams setId(String id) {
            return super.setId(id);
        }

        @Parameter(names = {"--region"}, description = VariantQueryParam.REGION_DESCR)
        @Override
        public AnnotationVariantQueryParams setRegion(String region) {
            return super.setRegion(region);
        }

        @Parameter(names = {"--gene"}, description = VariantQueryParam.GENE_DESCR)
        @Override
        public AnnotationVariantQueryParams setGene(String gene) {
            return super.setGene(gene);
        }

        @Parameter(names = {"--type"}, description = VariantQueryParam.TYPE_DESCR)
        @Override
        public AnnotationVariantQueryParams setType(String type) {
            return super.setType(type);
        }

        @Parameter(names = {"--panel"}, description = PANEL_DESC)
        @Override
        public AnnotationVariantQueryParams setPanel(String panel) {
            return super.setPanel(panel);
        }

        @Parameter(names = {"--cohort-stats-ref"}, description = VariantQueryParam.STATS_REF_DESCR)
        @Override
        public AnnotationVariantQueryParams setCohortStatsRef(String cohortStatsRef) {
            return super.setCohortStatsRef(cohortStatsRef);
        }

        @Parameter(names = {"--cohort-stats-alt"}, description = VariantQueryParam.STATS_ALT_DESCR)
        @Override
        public AnnotationVariantQueryParams setCohortStatsAlt(String cohortStatsAlt) {
            return super.setCohortStatsAlt(cohortStatsAlt);
        }

        @Parameter(names = {"--cohort-stats-maf"}, description = VariantQueryParam.STATS_MAF_DESCR)
        @Override
        public AnnotationVariantQueryParams setCohortStatsMaf(String cohortStatsMaf) {
            return super.setCohortStatsMaf(cohortStatsMaf);
        }

        @Parameter(names = {"--ct", "--consequence-type"}, description = VariantQueryParam.ANNOT_CONSEQUENCE_TYPE_DESCR)
        @Override
        public AnnotationVariantQueryParams setCt(String ct) {
            return super.setCt(ct);
        }

        @Parameter(names = {"--xref"}, description = VariantQueryParam.ANNOT_XREF_DESCR)
        @Override
        public AnnotationVariantQueryParams setXref(String xref) {
            return super.setXref(xref);
        }

        @Parameter(names = {"--biotype"}, description = VariantQueryParam.ANNOT_BIOTYPE_DESCR)
        @Override
        public AnnotationVariantQueryParams setBiotype(String biotype) {
            return super.setBiotype(biotype);
        }

        @Parameter(names = {"--protein-substitution"}, description = VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION_DESCR)
        @Override
        public AnnotationVariantQueryParams setProteinSubstitution(String proteinSubstitution) {
            return super.setProteinSubstitution(proteinSubstitution);
        }

        @Parameter(names = {"--conservation"}, description = VariantQueryParam.ANNOT_CONSERVATION_DESCR)
        @Override
        public AnnotationVariantQueryParams setConservation(String conservation) {
            return super.setConservation(conservation);
        }

        @Parameter(names = {"--population-frequency-maf"}, description = VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR)
        @Override
        public AnnotationVariantQueryParams setPopulationFrequencyMaf(String populationFrequencyMaf) {
            return super.setPopulationFrequencyMaf(populationFrequencyMaf);
        }

        @Parameter(names = {"--population-frequency-alt"}, description = VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR)
        @Override
        public AnnotationVariantQueryParams setPopulationFrequencyAlt(String populationFrequencyAlt) {
            return super.setPopulationFrequencyAlt(populationFrequencyAlt);
        }

        @Parameter(names = {"--population-frequency-ref"}, description = VariantQueryParam.ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR)
        @Override
        public AnnotationVariantQueryParams setPopulationFrequencyRef(String populationFrequencyRef) {
            return super.setPopulationFrequencyRef(populationFrequencyRef);
        }

        @Parameter(names = {"--transcript-flag"}, description = VariantQueryParam.ANNOT_TRANSCRIPT_FLAG_DESCR)
        @Override
        public AnnotationVariantQueryParams setTranscriptFlag(String transcriptFlag) {
            return super.setTranscriptFlag(transcriptFlag);
        }

        @Parameter(names = {"--functional-score"}, description = VariantQueryParam.ANNOT_FUNCTIONAL_SCORE_DESCR)
        @Override
        public AnnotationVariantQueryParams setFunctionalScore(String functionalScore) {
            return super.setFunctionalScore(functionalScore);
        }

        @Parameter(names = {"--clinical-significance"}, description = VariantQueryParam.ANNOT_CLINICAL_SIGNIFICANCE_DESCR)
        @Override
        public AnnotationVariantQueryParams setClinicalSignificance(String clinicalSignificance) {
            return super.setClinicalSignificance(clinicalSignificance);
        }
    }

    @Parameters(commandNames = {"histogram"}, commandDescription = "")
    public class VariantHistogramCommandOptions {

        @ParametersDelegate
        public BasicVariantQueryOptions variantQueryOptions = new BasicVariantQueryOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--sample"}, description = "List of samples to check. By default, all samples")
        public String samples;

        @Parameter(names = {"--interval"}, description = "")
        public Integer interval = 1000;

        @Parameter(names = {"-o", "--output"}, description = "Output file. [STDOUT]", arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = GwasCommandOptions.GWAS_RUN_COMMAND, commandDescription = GwasAnalysis.DESCRIPTION)
    public class GwasCommandOptions {
        public static final String GWAS_RUN_COMMAND = GwasAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--phenotype"}, description = "Use this phenotype to divide all the samples from the study. "
                + "Samples with the phenotype will be used as Case Cohort. Rest will be used as Control Cohort. "
                + "This parameter can not be mixed with other parameters to define the cohorts.")
        public String phenotype;

        @Parameter(names = {"--index"}, description = "Index the produced gwas score in the variant storage.", arity = 0)
        public boolean index;

        @Parameter(names = {"--index-score-id"}, description = "Name to be used to index que score in the variant storage. "
                + "Must be unique in the study. If provided, the control/case cohorts must be registered in catalog.")
        public String indexScoreId;

        @Parameter(names = {"--method"}, description = "Either Fisher Test or ChiSquare")
        public GwasConfiguration.Method method = GwasConfiguration.Method.FISHER_TEST;

        @Parameter(names = {"--fisher-mode"}, description = "Fisher Test mode.")
        public GwasConfiguration.FisherMode fisherMode = GwasConfiguration.FisherMode.TWO_SIDED;

        @Parameter(names = {"--case-cohort"}, description = "Cohort from catalog to be used as case cohort.")
        public String caseCohort;

        @Parameter(names = {"--case-cohort-samples"}, description = "List of samples to conform the case cohort.")
        public List<String> caseCohortSamples;

        @Parameter(names = {"--case-cohort-samples-annotation"}, description = "Samples annotation query selecting samples of the case cohort. "
                + "This parameter is an alternative to --case-cohort . Example: age>30;gender=FEMALE. "
                + "For more information, please visit " + ParamConstants.ANNOTATION_DOC_URL)
        public String caseCohortSamplesAnnotation;

        @Parameter(names = {"--control-cohort"}, description = "Cohort from catalog to be used as control cohort.")
        public String controlCohort;

        @Parameter(names = {"--control-cohort-samples"}, description = "List of samples to conform the control cohort.")
        public List<String> controlCohortSamples;

        @Parameter(names = {"--control-cohort-samples-annotation"}, description = "Samples query selecting samples of the control cohort. "
                + "This parameter is an alternative to --control-cohort . Example: age>30;gender=FEMALE. "
                + "For more information, please visit " + ParamConstants.ANNOTATION_DOC_URL)
        public String controlCohortSamplesAnnotation;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1, required = false)
        public String outdir;
    }

    @Parameters(commandNames = SAMPLE_VARIANT_STATS_RUN_COMMAND, commandDescription = SampleVariantStatsAnalysis.DESCRIPTION)
    public class SampleVariantStatsCommandOptions {
        public static final String SAMPLE_VARIANT_STATS_RUN_COMMAND = "sample-stats-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--sample"}, description = "List of samples.")
        public List<String> sample;

        @Parameter(names = {"--individual"}, description = "List of individuals")
        public List<String> individual;

        @DynamicParameter(names = {"--vq", "--variant-query"}, description = "Variant query, e.g.:. --vsq gene=\"BRCA2\" --vsq ct=\"missense_variant\"")
        public Map<String, String> variantQuery = new HashMap<>();

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1, required = false)
        public String outdir;

        @Parameter(names = {"--batch-size"}, description = "Number of samples to compute in parallel.")
        public Integer batchSize;

        @Parameter(names = {"--index"}, description = "Index result in catalog as sampleQc.", arity = 0)
        public boolean index;

        @Parameter(names = {"--index-id"}, description = "SampleQC identifier")
        public String indexId;

        @Parameter(names = {"--index-description"}, description = "SampleQC description")
        public String indexDescription;

        @Parameter(names = {"--index-overwrite"}, description = "Overwrite already indexed sample stats", arity = 0)
        public boolean indexOverwrite;
    }

    @Parameters(commandNames = SampleVariantStatsQueryCommandOptions.SAMPLE_VARIANT_STATS_QUERY_COMMAND, commandDescription = "Read precomputed sample variant stats")
    public class SampleVariantStatsQueryCommandOptions {
        public static final String SAMPLE_VARIANT_STATS_QUERY_COMMAND = "sample-stats-query";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--sample"}, description = "List of samples.", required = true)
        public List<String> sample;
    }

    @Parameters(commandNames = COHORT_VARIANT_STATS_RUN_COMMAND, commandDescription = CohortVariantStatsAnalysis.DESCRIPTION)
    public class CohortVariantStatsCommandOptions {
        public static final String COHORT_VARIANT_STATS_RUN_COMMAND = "cohort-stats-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--cohort"}, description = "Cohort name.")
        public String cohort;

        @Parameter(names = {"--samples"}, description = "List of samples.")
        public List<String> samples;

        @Parameter(names = {"--samples-annotation"}, description = "Samples query selecting samples of the control cohort."
                + " Example: age>30;gender=FEMALE."
                + " For more information, please visit " + ParamConstants.ANNOTATION_DOC_URL)
        public String samplesAnnotation;

        @Parameter(names = {"--index"}, description = "Index results in catalog. Requires a cohort."
                + "Create an AnnotationSet for the VariableSet " + CohortVariantStatsAnalysis.VARIABLE_SET_ID)
        public boolean index;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1, required = false)
        public String outdir;
    }

    @Parameters(commandNames = CohortVariantStatsQueryCommandOptions.COHORT_VARIANT_STATS_QUERY_COMMAND, commandDescription = "Read precomputed cohort variant stats")
    public class CohortVariantStatsQueryCommandOptions {
        public static final String COHORT_VARIANT_STATS_QUERY_COMMAND = "cohort-stats-query";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--cohort"}, description = "List of cohorts.", required = true)
        public List<String> cohort;
    }

    @Parameters(commandNames = KnockoutCommandOptions.KNOCKOUT_RUN_COMMAND, commandDescription = KnockoutAnalysis.DESCRIPTION)
    public class KnockoutCommandOptions {
        public static final String KNOCKOUT_RUN_COMMAND = "knockout-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = ParamConstants.STUDY_DESCRIPTION)
        public String study;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1, required = false)
        public String outdir;

        @Parameter(names = {"--sample"}, description = "List of samples to analyse. The analysis will produce a file for each sample.")
        public List<String> sample;

        @Parameter(names = {"--gene"}, description = "List of genes of interest. In combination with parameter panel, all genes will be used.")
        public List<String> gene;

        @Parameter(names = {"--panel"}, description = "List of panels of interest. In combination with parameter gene, all genes will be used.")
        public List<String> panel;

        @Parameter(names = {"--biotype"}, description = "List of biotypes. Used to filter by transcripts biotype.")
        public String biotype;

        @Parameter(names = {"--ct", "--consequence-type"}, description = "Consequence type as a list of SequenceOntology terms. "
                + "This filter is only applied on protein_coding genes. "
                + "By default filters by loss of function + missense_variant consequence types.")
        public String consequenceType;

        @Parameter(names = {"--filter"}, description = VariantQueryParam.FILTER_DESCR)
        public String filter;

        @Parameter(names = {"--qual"}, description = VariantQueryParam.QUAL_DESCR)
        public String qual;

        @Parameter(names = {"--skip-genes-file"}, description = "Do not generate the results file by gene")
        public boolean skipGenesFile;

        @Parameter(names = {"--index"}, description = "Index result for Recessive Gene Analysis")
        public boolean index;
    }

    @Parameters(commandNames = SampleEligibilityCommandOptions.SAMPLE_ELIGIBILITY_RUN_COMMAND, commandDescription = SampleEligibilityAnalysis.DESCRIPTION)
    public class SampleEligibilityCommandOptions {
        public static final String SAMPLE_ELIGIBILITY_RUN_COMMAND = "sample-eligibility-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = ParamConstants.STUDY_DESCRIPTION)
        public String study;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1, required = false)
        public String outdir;

        @Parameter(names = {"--query"}, description = "Election query. e.g. ((gene=A AND ct=lof) AND (NOT (gene=B AND ct=lof)))")
        public String query;

        @Parameter(names = {"--index"}, description = "Create a cohort with the resulting set of samples (if any)")
        public boolean index;

        @Parameter(names = {"--cohort-id"}, description = "The name of the cohort to be created")
        public String cohortId;
    }

    @Parameters(commandNames = MutationalSignatureCommandOptions.MUTATIONAL_SIGNATURE_RUN_COMMAND, commandDescription = MutationalSignatureAnalysis.DESCRIPTION)
    public class MutationalSignatureCommandOptions {
        public static final String MUTATIONAL_SIGNATURE_RUN_COMMAND = MutationalSignatureAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to.")
        public String study;

        @Parameter(names = {"--sample"}, description = "Sample name.")
        public String sample;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1, required = false)
        public String outdir;
    }

    @Parameters(commandNames = MendelianErrorCommandOptions.MENDELIAN_ERROR_RUN_COMMAND, commandDescription = MendelianErrorAnalysis.DESCRIPTION)
    public class MendelianErrorCommandOptions {
        public static final String MENDELIAN_ERROR_RUN_COMMAND = MendelianErrorAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to.")
        public String study;

        @Parameter(names = {"--family"}, description = "Family ID.")
        public String family;

        @Parameter(names = {"--individual"}, description = "Individual ID to get the family.")
        public String individual;

        @Parameter(names = {"--sample"}, description = "Sample ID to get the family.")
        public String sample;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1, required = false)
        public String outdir;
    }

    @Parameters(commandNames = InferredSexCommandOptions.INFERRED_SEX_RUN_COMMAND, commandDescription = InferredSexAnalysis.DESCRIPTION)
    public class InferredSexCommandOptions {
        public static final String INFERRED_SEX_RUN_COMMAND = InferredSexAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study ID where the individual or sample belong to.")
        public String study;

        @Parameter(names = {"--individual"}, description = "Individual ID.")
        public String individual;

        @Parameter(names = {"--sample"}, description = "Sample ID.")
        public String sample;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1, required = false)
        public String outdir;
    }

    @Parameters(commandNames = RelatednessCommandOptions.RELATEDNESS_RUN_COMMAND, commandDescription = RelatednessAnalysis.DESCRIPTION)
    public class RelatednessCommandOptions {
        public static final String RELATEDNESS_RUN_COMMAND = RelatednessAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study ID where all the individuals or samples belong to.")
        public String study;

        @Parameter(names = {"--individuals"}, description = "List of individual IDs (incompatible with --samples).")
        public List<String> individuals;

        @Parameter(names = {"--samples"}, description = "List of sample IDs (incompatible with --individuals).")
        public List<String> samples;

        @Parameter(names = {"--maf", "--minor-allele-freq"}, description = "Minor allele frequency to filter variants, e.g.: 1kg_phase3:CEU<0.35, cohort:ALL<0.4")
        public String minorAlleleFreq;

        @Parameter(names = {"--method"}, description = "Method to compute relatedness.")
        public String method = "IBD";

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = FamilyQcCommandOptions.FAMILY_QC_RUN_COMMAND, commandDescription = FamilyQcAnalysis.DESCRIPTION)
    public class FamilyQcCommandOptions {
        public static final String FAMILY_QC_RUN_COMMAND = FamilyQcAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to.")
        public String study;

        @Parameter(names = {"--family"}, description = "Family ID.", required = true)
        public String family;

        @Parameter(names = {"--relatedness-method"}, description = "Method to compute relatedness.")
        public String relatednessMethod = "PLINK/IBD";

        @Parameter(names = {"--relatedness-maf"}, description = "Minor allele frequency to filter variants, e.g.: 1kg_phase3:CEU>0.35, cohort:ALL>0.05")
        public String relatednessMaf = "cohort:ALL>0.05";

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.")
        public String outdir;
    }

    @Parameters(commandNames = IndividualQcCommandOptions.INDIVIDUAL_QC_RUN_COMMAND, commandDescription = IndividualQcAnalysis.DESCRIPTION)
    public class IndividualQcCommandOptions {
        public static final String INDIVIDUAL_QC_RUN_COMMAND = IndividualQcAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to.")
        public String study;

        @Parameter(names = {"--individual"}, description = "Individual ID.", required = true)
        public String individual;

        @Parameter(names = {"--sample"}, description = "Sample ID (in case that individual has multiple samples).")
        public String sample;

        @Parameter(names = {"--inferred-sex-method"}, description = "Method to infer sex. Valid values: " + IndividualQcAnalysisExecutor.COVERAGE_RATIO_INFERRED_SEX_METHOD)
        public String inferredSexMethod = IndividualQcAnalysisExecutor.COVERAGE_RATIO_INFERRED_SEX_METHOD;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.")
        public String outdir;
    }

    @Parameters(commandNames = SampleQcCommandOptions.SAMPLE_QC_RUN_COMMAND, commandDescription = SampleQcAnalysis.DESCRIPTION)
    public class SampleQcCommandOptions {
        public static final String SAMPLE_QC_RUN_COMMAND = SampleQcAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to.")
        public String study;

        @Parameter(names = {"--sample"}, description = "Sample ID.", required = true)
        public String sample;

        @Parameter(names = {"--variant-stats-id"}, description = "Variant stats ID.")
        public String variantStatsId;

        @Parameter(names = {"--variant-stats-description"}, description = "Variant stats description.")
        public String variantStatsDecription;

        @DynamicParameter(names = {"--vsq", "--variant-stats-query"}, description = "Variant stats query, e.g.:. --vsq gene=\"BRCA2\" --vsq ct=\"missense_variant\"")
        public Map<String, String> variantStatsQuery = new HashMap<>();

        @Parameter(names = {"--signature-id"}, description = "Signature ID.")
        public String signatureId;

        @DynamicParameter(names = {"--sq", "--signature-query"}, description = "Signature query, e.g.:. --sq type=\"SNV\" --sq ct=\"missense_variant\"")
        public Map<String, String> signatureQuery = new HashMap<>();

        @Parameter(names = {"--genes-for-coverage-stats"}, description = "A comma separated list of genes to compute the coverage stats.")
        public String genesForCoverageStats;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.")
        public String outdir;
    }


    @Parameters(commandNames = JulieRunCommandOptions.JULIE_RUN_COMMAND, commandDescription = JulieRunCommandOptions.DESCRIPTION)
    public class JulieRunCommandOptions extends GenericJulieRunCommandOptions {
        public static final String JULIE_RUN_COMMAND = JulieTool.ID + "-run";
        public static final String DESCRIPTION = JulieTool.DESCRIPTION;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

    }

    public static class GenericJulieRunCommandOptions {
        @Parameter(names = {"--project"}, description = PROJECT_DESC)
        public String project;

        @Parameter(names = {"--cohort"}, description = "List of cohorts from multiple studies with {study}:{cohort}")
        public String cohort;

        @Parameter(names = {"--region"}, description = "Region to process")
        public String region;

        @Parameter(names = {"--overwrite"}, description = "Overwrite all population frequencies.")
        public boolean overwrite;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1, required = false)
        public String outdir;
    }

    //-------------------------------------------------------------------------
    // W R A P P E R S     A N A L Y S I S
    //-------------------------------------------------------------------------

    // Plink

    @Parameters(commandNames = PlinkCommandOptions.PLINK_RUN_COMMAND, commandDescription = PlinkWrapperAnalysis.DESCRIPTION)
    public class PlinkCommandOptions {
        public static final String PLINK_RUN_COMMAND = PlinkWrapperAnalysis.ID + "-run";

        @ParametersDelegate
        //public GeneralCliOptions.CommonCommandOptions commonOptions = commonOptions;
        public GeneralCliOptions.CommonCommandOptions basicOptions = commonCommandOptions;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study.")
        public String study;

        @Parameter(names = {"--tped-file"}, description = "Transpose PED file (.tped) containing SNP and genotype information.")
        public String tpedFile;

        @Parameter(names = {"--tfam-file"}, description = "Transpose FAM file (.tfam) containing individual and family information.")
        public String tfamFile;

        @Parameter(names = {"--covar-file"}, description = "Covariate file.")
        public String covarFile;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.")
        public String outdir;
    }

    // RvTests

    @Parameters(commandNames = RvtestsCommandOptions.RVTESTS_RUN_COMMAND, commandDescription = RvtestsWrapperAnalysis.DESCRIPTION)
    public class RvtestsCommandOptions {
        public static final String RVTESTS_RUN_COMMAND = RvtestsWrapperAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions basicOptions = commonCommandOptions;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @Parameter(names = {"--study"}, description = "Study.")
        public String study;

        @Parameter(names = {"--command"}, description = "Rvtests command. Valid values: rvtest or vcf2kinship.")
        public String command = "rvtest";

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.")
        public String outdir;

        @DynamicParameter(names = {"--rvtests-params"}, description = "RvTests parameters e.g.:. --rvtests-params single=famScore --rvtests-params meta=score,cov")
        public Map<String, String> rvtestsParams = new HashMap<>();
    }

    // GATK

    @Parameters(commandNames = org.opencb.opencga.analysis.wrappers.gatk.GatkWrapperAnalysis.ID, commandDescription = org.opencb.opencga.analysis.wrappers.gatk.GatkWrapperAnalysis.DESCRIPTION)
    public class GatkCommandOptions {
        public static final String GATK_RUN_COMMAND = GatkWrapperAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions basicOptions = commonCommandOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-s", "--study"}, description = STUDY_DESCRIPTION, arity = 1)
        public String study;

        @Parameter(names = {"--command"}, description = GATK_COMMAND_DESCRIPTION)
        public String command;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;

        @DynamicParameter(names = {"--gatk-params"}, description = "Gatk parameters e.g.:. --gatk-params I=test.bam")
        public Map<String, String> gatkParams = new HashMap<>();
    }
}
