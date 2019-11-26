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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.converters.CommaParameterSplitter;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.opencga.analysis.variant.gwas.GwasAnalysis;
import org.opencb.opencga.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.DataModelOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.NumericOptions;
import org.opencb.opencga.app.cli.main.options.SampleCommandOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;

import java.util.List;

import static org.opencb.opencga.analysis.variant.VariantCatalogQueryUtils.*;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.CohortVariantStatsCommandOptions.COHORT_VARIANT_STATS_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.FamilyIndexCommandOptions.FAMILY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.FamilyIndexCommandOptions.FAMILY_INDEX_COMMAND_DESCRIPTION;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleIndexCommandOptions.SAMPLE_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleIndexCommandOptions.SAMPLE_INDEX_COMMAND_DESCRIPTION;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleVariantStatsCommandOptions.SAMPLE_VARIANT_STATS_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSecondaryIndexCommandOptions.SECONDARY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSecondaryIndexDeleteCommandOptions.SECONDARY_INDEX_DELETE_COMMAND;
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
//    public final QueryVariantCommandOptionsOld queryVariantCommandOptionsOld;
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
    public final VariantSamplesFilterCommandOptions samplesFilterCommandOptions;
    public final VariantHistogramCommandOptions histogramCommandOptions;

    // Analysis
    public final GwasCommandOptions gwasCommandOptions;
    public final SampleVariantStatsCommandOptions sampleVariantStatsCommandOptions;
    public final SampleVariantStatsQueryCommandOptions sampleVariantStatsQueryCommandOptions;
    public final CohortVariantStatsCommandOptions cohortVariantStatsCommandOptions;
    public final CohortVariantStatsQueryCommandOptions cohortVariantStatsQueryCommandOptions;

    public final JCommander jCommander;
    public final GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    public final DataModelOptions commonDataModelOptions;
    public final NumericOptions commonNumericOptions;

    public VariantCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, DataModelOptions dataModelOptions,
                                 NumericOptions numericOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.commonDataModelOptions = dataModelOptions;
        this.commonNumericOptions = numericOptions;
        this.jCommander = jCommander;

        this.indexVariantCommandOptions = new VariantIndexCommandOptions();
        this.variantDeleteCommandOptions = new VariantDeleteCommandOptions();
        this.variantSecondaryIndexCommandOptions = new VariantSecondaryIndexCommandOptions();
        this.variantSecondaryIndexDeleteCommandOptions = new VariantSecondaryIndexDeleteCommandOptions();
//        this.queryVariantCommandOptionsOld = new QueryVariantCommandOptionsOld();
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
        this.samplesFilterCommandOptions = new VariantSamplesFilterCommandOptions();
        this.histogramCommandOptions = new VariantHistogramCommandOptions();
        this.gwasCommandOptions = new GwasCommandOptions();
        this.sampleVariantStatsCommandOptions = new SampleVariantStatsCommandOptions();
        this.sampleVariantStatsQueryCommandOptions = new SampleVariantStatsQueryCommandOptions();
        this.cohortVariantStatsCommandOptions = new CohortVariantStatsCommandOptions();
        this.cohortVariantStatsQueryCommandOptions = new CohortVariantStatsQueryCommandOptions();
    }

    @Parameters(commandNames = {"index"}, commandDescription = "Index variants file")
    public class VariantIndexCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GenericVariantIndexOptions genericVariantIndexOptions = new GenericVariantIndexOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--file"}, description = "CSV of file ids to be indexed", required = true, arity = 1)
        public String fileId = null;

        @Parameter(names = {"--transformed-files"}, description = "CSV of paths corresponding to the location of the transformed files.",
                arity = 1)
        public String transformedPaths = null;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory outside catalog boundaries.", required = true, arity = 1)
        public String outdir = null;

        @Parameter(names = {"--stdin"}, description = "Read the variants file from the standard input")
        public boolean stdin;

        @Parameter(names = {"--stdout"}, description = "Write the transformed variants file to the standard output")
        public boolean stdout;
    }

    @Parameters(commandNames = {SECONDARY_INDEX_COMMAND}, commandDescription = "Creates a secondary index using a search engine")
    public class VariantSecondaryIndexCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String SECONDARY_INDEX_COMMAND = "secondary-index";
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project"}, description = "Project to index.", arity = 1)
        public String project;

        @Parameter(names = {"-r", "--region"}, description = VariantQueryParam.REGION_DESCR)
        public String region;

        @Parameter(names = {"--sample"}, description = "Samples to index."
                + " If provided, all sample data will be added to the secondary index.", arity = 1)
        public String sample;

        @Parameter(names = {"--overwrite"}, description = "Overwrite search index for all files and variants. Repeat operation for already processed variants.")
        public boolean overwrite;

        // TODO Use this outdir to store the operation-status.json
        @Parameter(names = {"--outdir"}, description = "[PENDING]", hidden = true)
        public String outdir;
    }

    @Parameters(commandNames = {SECONDARY_INDEX_DELETE_COMMAND}, commandDescription = "Remove a secondary index from the search engine")
    public class VariantSecondaryIndexDeleteCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String SECONDARY_INDEX_DELETE_COMMAND = "secondary-index-delete";
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--sample"}, description = "Samples to remove. Needs to provide all the samples in the secondary index.",
                required = true, arity = 1)
        public String sample;

        // TODO Use this outdir to store the operation-status.json
        @Parameter(names = {"--outdir"}, description = "[PENDING]", hidden = true)
        public String outdir;
    }

    @Deprecated
    public class IndexVariantCommandOptionsOld extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

//        @ParametersDelegate
//        public AnalysisCliOptionsParser.JobCommand job = new AnalysisCliOptionsParser.JobCommand();

//
//        @Parameter(names = {"-i", "--input"}, description = "File to index in the selected backend", required = true, variableArity = true)
//        public List<String> input;

//        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved (optional)", arity = 1)
//        public String outdir;

//        @Parameter(names = {"--file-id"}, description = "Unique ID for the file", required = true, arity = 1)
//        public String fileId;


//        @Parameter(names = {"--study-id"}, description = "Unque ID for the study", arity = 1)
//        public long studyId;

        @Parameter(names = {"--file"}, description = "CSV of file ids to be indexed", required = true, arity = 1)
        public String fileId = null;

        @Parameter(names = {"--transformed-files"}, description = "CSV of paths corresponding to the location of the transformed files.",
                arity = 1)
        public String transformedPaths = null;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory outside catalog boundaries.", required = true, arity = 1)
        public String outdir = null;


        //////
        // Commons

        @Parameter(names = {"--transform"}, description = "If present it only runs the transform stage, no load is executed")
        public boolean transform = false;

        @Parameter(names = {"--load"}, description = "If present only the load stage is executed, transformation is skipped")
        public boolean load = false;

        @Parameter(names = {"--exclude-genotypes"}, description = "Index excluding the genotype information")
        public boolean excludeGenotype = false;

        @Parameter(names = {"--include-extra-fields"}, description = "Index including other genotype fields [CSV]")
        public String extraFields = "";

        @Parameter(names = {"--aggregated"}, description = "Select the type of aggregated VCF file: none, basic, EVS or ExAC", arity = 1)
        public Aggregation aggregated = Aggregation.NONE;

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF " +
                "file")
        public String aggregationMappingFile = null;

        @Parameter(names = {"--gvcf"}, description = "The input file is in gvcf format")
        public boolean gvcf;

        @Parameter(names = {"--bgzip"}, description = "[PENDING] The input file is in bgzip format")
        public boolean bgzip;

        @Parameter(names = {"--calculate-stats"}, description = "Calculate indexed variants statistics after the load step")
        public boolean calculateStats = false;

        @Parameter(names = {"--annotate"}, description = "Annotate indexed variants after the load step")
        public boolean annotate = false;

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
        public VariantAnnotatorFactory.AnnotationEngine annotator = null;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        public boolean overwriteAnnotations;

        @Parameter(names = {"--resume"}, description = "Resume a previously failed indexation", arity = 0)
        public boolean resume;
    }

    @Parameters(commandNames = {VARIANT_DELETE_COMMAND}, commandDescription = VARIANT_DELETE_COMMAND_DESCRIPTION)
    public class VariantDeleteCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GenericVariantDeleteOptions genericVariantDeleteOptions = new GenericVariantDeleteOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;
    }

    @Deprecated
    public class QueryVariantCommandOptionsOld {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--studies"}, description = "Study identifiers", required = true, arity = 1)
        public String studies;

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"--variant-ids"}, description = "List of variant ids", arity = 1)
        public String ids;

        @Parameter(names = {"--region"}, description = "List of regions: {chr}:{start}-{end}", arity = 1)
        public String region;

        @Parameter(names = {"--chromosome"}, description = "List of chromosomes", arity = 1)
        public String chromosome;

        @Parameter(names = {"--gene"}, description = "List of genes", arity = 1)
        public String gene;

        @Parameter(names = {"--type"}, description = "Variant types: [SNV, MNV, INDEL, SV, CNV]", arity = 1)
        public VariantType type;

        @Parameter(names = {"--reference"}, description = "Reference allele", arity = 1)
        public String reference;

        @Parameter(names = {"--alternate"}, description = "Main alternate allele", arity = 1)
        public String alternate;

        @Parameter(names = {"--returned-studies"}, description = "List of studies to be returned", arity = 1)
        public String returnedStudies;

        @Parameter(names = {"--returned-samples"}, description = "List of samples to be returned", arity = 1)
        public String returnedSamples;

        @Parameter(names = {"--returned-files"}, description = "List of files to be returned.", arity = 1)
        public String returnedFiles;

        @Parameter(names = {"--files"}, description = "Variants in specific files", arity = 1)
        public String files;

        @Parameter(names = {"--maf"}, description = "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}",
                arity = 1)
        public String maf;

        @Parameter(names = {"--mgf"}, description = "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}",
                arity = 1)
        public String mgf;

        @Parameter(names = {"--missing-alleles"}, description = "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}",
                arity = 1)
        public String missingAlleles;

        @Parameter(names = {"--missing-genotypes"}, description = "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}",
                arity = 1)
        public String missingGenotypes;

//        @Parameter(names = {"--annotation-exists"}, description = "Specify if the variant annotation must exists.",
//                arity = 0)
//        public boolean annotationExists;

        @Parameter(names = {"--genotype"}, description = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}"
                + "(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1", arity = 1)
        public String genotype;

        @Parameter(names = {"--annot-ct"}, description = "Consequence type SO term list. e.g. missense_variant,stop_lost or SO:0001583,SO:0001578",
                arity = 1)
        public String annot_ct;

        @Parameter(names = {"--annot-xref"}, description = "XRef", arity = 1)
        public String annot_xref;

        @Parameter(names = {"--annot-biotype"}, description = "Biotype", arity = 1)
        public String annot_biotype;

        @Parameter(names = {"--polyphen"}, description = "Polyphen, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description}"
                + " e.g. <=0.9 , =benign", arity = 1)
        public String polyphen;

        @Parameter(names = {"--sift"}, description = "Sift, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} "
                + "e.g. >0.1 , ~=tolerant", arity = 1)
        public String sift;

        @Parameter(names = {"--conservation"}, description = "VConservation score: {conservation_score}[<|>|<=|>=]{number} "
                + "e.g. phastCons>0.5,phylop<0.1,gerp>0.1", arity = 1)
        public String conservation;

        @Parameter(names = {"--annot-population-maf"}, description = "Population minor allele frequency: "
                + "{study}:{population}[<|>|<=|>=]{number}", arity = 1)
        public String annotPopulationMaf;

        @Parameter(names = {"--alternate-frequency"}, description = "Alternate Population Frequency: "
                + "{study}:{population}[<|>|<=|>=]{number}", arity = 1)
        public String alternate_frequency;

        @Parameter(names = {"--reference-frequency"}, description = "Reference Population Frequency:"
                + " {study}:{population}[<|>|<=|>=]{number}", arity = 1)
        public String reference_frequency;

        @Parameter(names = {"--annot-transcription-flags"}, description = "List of transcript annotation flags. "
                + "e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno", arity = 1)
        public String transcriptionFlags;

        @Parameter(names = {"--annot-gene-trait-id"}, description = "List of gene trait association id. e.g. \"umls:C0007222\" , "
                + "\"OMIM:269600\"", arity = 1)
        public String geneTraitId;


        @Parameter(names = {"--annot-gene-trait-name"}, description = "List of gene trait association names. "
                + "e.g. \"Cardiovascular Diseases\"", arity = 1)
        public String geneTraitName;

        @Parameter(names = {"--annot-hpo"}, description = "List of HPO terms. e.g. \"HP:0000545\"", arity = 1)
        public String hpo;

        @Parameter(names = {"--annot-go"}, description = "List of GO (Genome Ontology) terms. e.g. \"GO:0002020\"", arity = 1)
        public String go;

        @Parameter(names = {"--annot-expression"}, description = "List of tissues of interest. e.g. \"tongue\"", arity = 1)
        public String expression;

        @Parameter(names = {"--annot-protein-keywords"}, description = "List of protein variant annotation keywords",
                arity = 1)
        public String proteinKeyword;

        @Parameter(names = {"--annot-drug"}, description = "List of drug names", arity = 1)
        public String drug;

        @Parameter(names = {"--annot-functional-score"}, description = "Functional score: {functional_score}[<|>|<=|>=]{number} "
                + "e.g. cadd_scaled>5.2 , cadd_raw<=0.3", arity = 1)
        public String functionalScore;

        @Parameter(names = {"--unknown-genotype"}, description = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]",
                arity = 1)
        public String unknownGenotype;

        @Parameter(names = {"--samples-metadata"}, description = "Returns the samples metadata group by study. Sample names will appear in the same order as their corresponding genotypes.",
                arity = 0)
        public boolean samplesMetadata;

        @Parameter(names = {"--sort"}, description = "Sort the results", arity = 0)
        public boolean sort;

        @Parameter(names = {"--group-by"}, description = "Group variants by: [ct, gene, ensemblGene]", arity = 1)
        public String groupBy;

        @Parameter(names = {"--count"}, description = "Count results", arity = 0)
        public boolean count;

        @Parameter(names = {"--histogram"}, description = "Calculate histogram. Requires one region.", arity = 0)
        public boolean histogram;

        @Parameter(names = {"--interval"}, description = "Histogram interval size. Default:2000", arity = 1)
        public String interval;

        @Parameter(names = {"--mode"}, description = "Communication mode. grpc|rest|auto.")
        public String mode = "auto";

    }

    @Parameters(commandNames = {"query"}, commandDescription = "Search over indexed variants")
    public class VariantQueryCommandOptions extends AbstractVariantQueryCommandOptions {

        // FIXME: This param should not be in the INTERNAL command line!
        @Parameter(names = {"--mode"}, description = "Communication mode. grpc|rest|auto.")
        public String mode = "auto";

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory. [STDOUT]", arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {"export"}, commandDescription = "Search over indexed variants")
    public class VariantExportCommandOptions extends AbstractVariantQueryCommandOptions {

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1)
        public String outdir;
    }

    public class AbstractVariantQueryCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GenericVariantQueryOptions genericVariantQueryOptions = new GenericVariantQueryOptions();

        @ParametersDelegate
        public GeneralCliOptions.BasicCommonCommandOptions commonOptions = new GeneralCliOptions.BasicCommonCommandOptions();

        @Parameter(names = {"--output-file-name"}, description = "Output file name.", arity = 1)
        public String outputFileName;

        @Parameter(names = {"--of", "--output-format"}, description = "Output format. one of {VCF, VCF.GZ, JSON, AVRO, PARQUET, STATS, CELLBASE}", arity = 1)
        public String outputFormat = "VCF";

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"--sample-filter"}, description = SAMPLE_ANNOTATION_DESC)
        public String sampleFilter;

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;

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

        // FIXME: This param should not be in the REST command line!
        @Parameter(names = {"--variants-file"}, description = "GFF File with regions")
        public String variantsFile;
    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Create and load stats into a database.")
    public class VariantStatsCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GenericVariantStatsOptions genericVariantStatsOptions = new GenericVariantStatsOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--cohort"}, description = "Cohort Ids for the cohorts to be calculated.")
        public List<String> cohort;

        @Parameter(names = {"--cohort-ids"}, hidden = true)
        public void setCohortIds(List<String> cohortIds) {
            this.cohort = cohortIds;
        }

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory outside catalog boundaries.", required = false, arity = 1)
        public String outdir = null;

        @Parameter(names = {"--index"}, description = "Index stats in the variant storage database", arity = 0)
        public boolean index;

        @Parameter(names = {"--samples"}, description = "List of samples to use as cohort to calculate stats")
        public List<String> samples;
    }

    @Parameters(commandNames = {GenericVariantScoreIndexCommandOptions.SCORE_INDEX_COMMAND}, commandDescription = GenericVariantScoreIndexCommandOptions.SCORE_INDEX_COMMAND_DESCRIPTION)
    public class VariantScoreIndexCommandOptions extends GenericVariantScoreIndexCommandOptions {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.StudyOption study = new GeneralCliOptions.StudyOption();
    }

    @Parameters(commandNames = {GenericVariantScoreDeleteCommandOptions.SCORE_DELETE_COMMAND}, commandDescription = GenericVariantScoreDeleteCommandOptions.SCORE_DELETE_COMMAND_DESCRIPTION)
    public class VariantScoreDeleteCommandOptions extends GenericVariantScoreDeleteCommandOptions {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.StudyOption study = new GeneralCliOptions.StudyOption();
    }

    @Parameters(commandNames = {SAMPLE_INDEX_COMMAND}, commandDescription = SAMPLE_INDEX_COMMAND_DESCRIPTION)
    public class SampleIndexCommandOptions extends GeneralCliOptions.StudyOption {
        public static final String SAMPLE_INDEX_COMMAND = "sample-index";
        public static final String SAMPLE_INDEX_COMMAND_DESCRIPTION = "Annotate sample index.";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--sample"}, required = true, description = "Samples to include in the index. " +
                "Use \"" + VariantQueryUtils.ALL + "\" to annotate the index for all samples in the study.")
        public String sample;

//        @Parameter(names = {"--overwrite"}, description = "Overwrite mendelian errors")
//        public boolean overwrite = false;
    }

    @Parameters(commandNames = {FAMILY_INDEX_COMMAND}, commandDescription = FAMILY_INDEX_COMMAND_DESCRIPTION)
    public class FamilyIndexCommandOptions extends GeneralCliOptions.StudyOption {
        public static final String FAMILY_INDEX_COMMAND = "family-index";
        public static final String FAMILY_INDEX_COMMAND_DESCRIPTION = "Build the family index.";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--family"}, required = true, description = "Families to index. " +
                "Use \"" + VariantQueryUtils.ALL + "\" to index all families in the study.")
        public String family;

        @Parameter(names = {"--overwrite"}, description = "Overwrite existing values")
        public boolean overwrite = false;
    }

    public class StatsVariantStatsCommandOptionsOld { //extends AnalysisCliOptionsParser.CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

//        @ParametersDelegate
//        public AnalysisCliOptionsParser.JobCommand job = new AnalysisCliOptionsParser.JobCommand();

//        @Parameter(names = {"--create"}, description = "Run only the creation of the stats to a file")
//        public boolean create = false;
//
//        @Parameter(names = {"--load"}, description = "Load the stats from an already existing FILE directly into the database. FILE is a "
//                + "prefix with structure <INPUT_FILENAME>.<TIME>")
//        public boolean load = false;

        @Parameter(names = {"--overwrite-stats"}, description = "Overwrite stats in variants already present")
        public boolean overwriteStats = false;

        @Parameter(names = {"--region"}, description = "[PENDING] Region to calculate.")
        public String region;

        @Parameter(names = {"--update-stats"}, description = "Calculate stats just for missing positions. "
                + "Assumes that existing stats are correct")
        public boolean updateStats = false;

        @Parameter(names = {"-s", "--study-id"}, description = "Unique ID for the study where the file is classified", required = true,
                arity = 1)
        public String studyId;

        @Parameter(names = {"-f", "--file-id"}, description = "Calculate stats only for the selected file", arity = 1)
        public String fileId;

        @Parameter(names = {"--cohort-ids"}, description = "Cohort Ids for the cohorts to be calculated.")
        public String cohortIds;

        // FIXME: Hidden?
        @Parameter(names = {"--output-filename"}, description = "Output file name. Default: database name", arity = 1)
        public String fileName;

//        @Parameter(names = {"--outdir-id"}, description = "Output directory", arity = 1)
//        public String outdirId;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory outside catalog boundaries.", required = true, arity = 1)
        public String outdir = null;

        @Parameter(names = {"--aggregated"}, description = "Select the type of aggregated VCF file: none, basic, EVS or ExAC", arity = 1)
        public Aggregation aggregated = Aggregation.NONE;

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF file")
        public String aggregationMappingFile;

        @Parameter(names = {"--resume"}, description = "Resume a previously failed stats calculation", arity = 0)
        public boolean resume;
    }

    @Parameters(commandNames = {"annotate"}, commandDescription = GenericVariantAnnotateOptions.ANNOTATE_DESCRIPTION)
    public class VariantAnnotateCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GenericVariantAnnotateOptions genericVariantAnnotateOptions = new GenericVariantAnnotateOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project-id"}, description = "Project to annotate.", arity = 1)
        public String project;

//        @Parameter(names = {"-s", "--study-id"}, description = "Studies to annotate. Must be in the same database.", arity = 1)
//        public String study;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory outside catalog boundaries.", required = true, arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = {ANNOTATION_SAVE_COMMAND}, commandDescription = ANNOTATION_SAVE_COMMAND_DESCRIPTION)
    public class AnnotationSaveCommandOptions extends GenericAnnotationSaveCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;
    }

    @Parameters(commandNames = {ANNOTATION_DELETE_COMMAND}, commandDescription = ANNOTATION_DELETE_COMMAND_DESCRIPTION)
    public class AnnotationDeleteCommandOptions extends GenericAnnotationDeleteCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;

    }

    @Parameters(commandNames = {ANNOTATION_QUERY_COMMAND}, commandDescription = ANNOTATION_QUERY_COMMAND_DESCRIPTION)
    public class AnnotationQueryCommandOptions extends GenericAnnotationQueryCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.DataModelOptions dataModelOptions = new DataModelOptions();

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

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;
    }

    @Deprecated
    public class AnnotateVariantCommandOptionsOld { //extends AnalysisCliOptionsParser.CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

//        @ParametersDelegate
//        public AnalysisCliOptionsParser.JobCommand job = new AnalysisCliOptionsParser.JobCommand();

        @Parameter(names = {"-p", "--project-id"}, description = "Project to annotate.", arity = 1)
        public String project;

        @Parameter(names = {"-s", "--study-id"}, description = "Studies to annotate. Must be in the same database.", arity = 1)
        public String studyId;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory outside catalog boundaries.", required = true, arity = 1)
        public String outdir = null;

        /////////
        // Generic

        @Parameter(names = {"--create"}, description = "Run only the creation of the annotations to a file (specified by --output-filename)")
        public boolean create = false;

        @Parameter(names = {"--load"}, description = "Run only the load of the annotations into the DB from FILE. "
                + "Can be a file from catalog or a local file.")
        public String load = null;

        @Parameter(names = {"--custom-name"}, description = "Provide a name to the custom annotation")
        public String customAnnotationKey = null;

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
        public VariantAnnotatorFactory.AnnotationEngine annotator;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        public boolean overwriteAnnotations = false;

        @Parameter(names = {"--output-filename"}, description = "Output file name. Default: dbName", arity = 1)
        public String fileName;

        @Parameter(names = {"--species"}, description = "Species. Default hsapiens", arity = 1)
        public String species = "hsapiens";

        @Parameter(names = {"--assembly"}, description = "Assembly. Default GRCh37", arity = 1)
        public String assembly = "GRCh37";

        @Parameter(names = {"--filter-region"}, description = "Comma separated region filters", splitter = CommaParameterSplitter.class)
        public List<String> filterRegion;

        @Parameter(names = {"--filter-chromosome"}, description = "Comma separated chromosome filters", splitter = CommaParameterSplitter.class)
        public List<String> filterChromosome;

        @Parameter(names = {"--filter-gene"}, description = "Comma separated gene filters", splitter = CommaParameterSplitter.class)
        public String filterGene;

        @Parameter(names = {"--filter-annot-consequence-type"}, description = "Comma separated annotation consequence type filters",
                splitter = CommaParameterSplitter.class)
        public List filterAnnotConsequenceType = null; // TODO will receive CSV, only available when create annotations

    }

    @Parameters(commandNames = {AGGREGATE_FAMILY_COMMAND}, commandDescription = AGGREGATE_FAMILY_COMMAND_DESCRIPTION)
    public class AggregateFamilyCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GenericAggregateFamilyOptions genericAggregateFamilyOptions = new GenericAggregateFamilyOptions();
    }

    @Parameters(commandNames = {AGGREGATE_COMMAND}, commandDescription = AGGREGATE_COMMAND_DESCRIPTION)
    public class AggregateCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GenericAggregateCommandOptions aggregateCommandOptions = new GenericAggregateCommandOptions();

    }

    @Parameters(commandNames = {"export-frequencies"}, commandDescription = "Export calculated variant stats and frequencies")
    public class VariantExportStatsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

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

        @Parameter(names = {"-s", "--study"}, description = "Study where to load the variants", required = true)
        public String study;

        @Parameter(names = {"-i", "--input"}, description = "Variants input file in avro format", required = true)
        public String input;

    }

    @Parameters(commandNames = {"ibs"}, commandDescription = "[EXPERIMENTAL] Identity By State Clustering")
    public class VariantIbsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--sample"}, description = "List of samples to check. By default, all samples")
        public String samples;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.")
        public String outdir = "-";
    }

    @Parameters(commandNames = {"samples"}, commandDescription = "Get samples given a set of variants")
    public class VariantSamplesFilterCommandOptions {

        @ParametersDelegate
        public BasicVariantQueryOptions variantQueryOptions = new BasicVariantQueryOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        //TODO
//        @Parameter(names = {"--sample-filter"}, description = SAMPLE_FILTER_DESC)
//        public String sampleFilter;

        @Parameter(names = {"--sample"}, description = "List of samples to check. By default, all samples")
        public String samples;

        @Parameter(names = {"--all"}, description = "Samples must be present in ALL variants or in ANY variant.")
        public boolean all;

        @Parameter(names = {"--genotypes"}, description = "Genotypes that the sample must have to be selected")
        public String genotypes = "0/1,1/1";



//        @Parameter(names = {"-o", "--output"}, description = "Output file. [STDOUT]", arity = 1)
//        public String output;
    }

    @Parameters(commandNames = {"histogram"}, commandDescription = "")
    public class VariantHistogramCommandOptions {

        @ParametersDelegate
        public BasicVariantQueryOptions variantQueryOptions = new BasicVariantQueryOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--sample"}, description = "List of samples to check. By default, all samples")
        public String samples;

        @Parameter(names = {"--interval"}, description = "")
        public Integer interval = 1000;

        @Parameter(names = {"-o", "--output"}, description = "Output file. [STDOUT]", arity = 1)
        public String outdir;
    }

    @Parameters(commandNames = GwasAnalysis.ID, commandDescription = GwasAnalysis.DESCRIPTION)
    public class GwasCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--phenotype"}, description = "Use this phenotype to divide all the samples from the study. "
                + "Samples with the phenotype will be used as Case Cohort. Rest will be used as Control Cohort. "
                + "This parameter can not be mixed with other parameters to define the cohorts.")
        public String phenotype;

        @Parameter(names = {"--index-score"}, description = "Name to be used to index que score in the variant storage. "
                + "Must be unique in the study. If provided, the control/case cohorts must be registered in catalog.")
        public String scoreName;

        @Parameter(names = {"--method"}, description = "Either Fisher Test or ChiSquare")
        public GwasConfiguration.Method method = GwasConfiguration.Method.FISHER_TEST;

        @Parameter(names = {"--fisher-mode"}, description = "Fisher Test mode.")
        public GwasConfiguration.FisherMode fisherMode = GwasConfiguration.FisherMode.TWO_SIDED;

        @Parameter(names = {"--case-cohort"}, description = "Cohort from catalog to be used as case cohort.")
        public String caseCohort;

        @Parameter(names = {"--case-samples-annotation"}, description = "Samples annotation query selecting samples of the case cohort. "
                + "This parameter is an alternative to --case-cohort . Example: age>30;gender=FEMALE. "
                + "For more information, please visit " + SampleCommandOptions.SearchCommandOptions.ANNOTATION_DOC_URL)
        public String caseSamplesAnnotation;

        @Parameter(names = {"--control-cohort"}, description = "Cohort from catalog to be used as control cohort.")
        public String controlCohort;

        @Parameter(names = {"--control-samples-annotation"}, description = "Samples query selecting samples of the control cohort. "
                + "This parameter is an alternative to --control-cohort . Example: age>30;gender=FEMALE. "
                + "For more information, please visit " + SampleCommandOptions.SearchCommandOptions.ANNOTATION_DOC_URL)
        public String controlSamplesAnnotation;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1, required = false)
        public String outdir;
    }

    @Parameters(commandNames = SAMPLE_VARIANT_STATS_COMMAND, commandDescription = SampleVariantStatsAnalysis.DESCRIPTION)
    public class SampleVariantStatsCommandOptions {
        public static final String SAMPLE_VARIANT_STATS_COMMAND = "sample-stats";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--sample"}, description = "List of samples.")
        public List<String> sample;

        @Parameter(names = {"--family"}, description = "Select samples form the individuals of this family..")
        public String family;

        @Parameter(names = {"--samples-annotation"}, description = "Samples query selecting samples of the control cohort."
                + " Example: age>30;gender=FEMALE."
                + " For more information, please visit " + SampleCommandOptions.SearchCommandOptions.ANNOTATION_DOC_URL)
        public String samplesAnnotation;

        @Parameter(names = {"--index-stats"}, description = "Index results in catalog."
                + "Create an AnnotationSet for the VariableSet " + SampleVariantStatsAnalysis.VARIABLE_SET_ID)
        public boolean index;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1, required = false)
        public String outdir;
    }

    @Parameters(commandNames = SampleVariantStatsQueryCommandOptions.SAMPLE_VARIANT_STATS_QUERY_COMMAND, commandDescription = "Read precomputed sample variant stats")
    public class SampleVariantStatsQueryCommandOptions {
        public static final String SAMPLE_VARIANT_STATS_QUERY_COMMAND = "sample-stats-query";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--sample"}, description = "List of samples.")
        public List<String> sample;
    }

    @Parameters(commandNames = COHORT_VARIANT_STATS_COMMAND, commandDescription = CohortVariantStatsAnalysis.DESCRIPTION)
    public class CohortVariantStatsCommandOptions {
        public static final String COHORT_VARIANT_STATS_COMMAND = "cohort-stats";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--cohort"}, description = "Cohort name.")
        public String cohort;

        @Parameter(names = {"--samples"}, description = "List of samples.")
        public List<String> samples;

        @Parameter(names = {"--samples-annotation"}, description = "Samples query selecting samples of the control cohort."
                + " Example: age>30;gender=FEMALE."
                + " For more information, please visit " + SampleCommandOptions.SearchCommandOptions.ANNOTATION_DOC_URL)
        public String samplesAnnotation;

        @Parameter(names = {"--index-stats"}, description = "Index results in catalog. Requires a cohort."
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

        @Parameter(names = {"--study"}, description = "Study where all the samples belong to")
        public String study;

        @Parameter(names = {"--cohort"}, description = "List of cohorts.")
        public List<String> cohort;
    }

}
