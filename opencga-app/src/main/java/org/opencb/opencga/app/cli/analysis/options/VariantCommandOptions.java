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

package org.opencb.opencga.app.cli.analysis.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.converters.CommaParameterSplitter;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.DataModelOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.NumericOptions;
import org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;

import java.util.List;

import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.FamilyIndexCommandOptions.FAMILY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.FamilyIndexCommandOptions.FAMILY_INDEX_COMMAND_DESCRIPTION;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.SampleIndexCommandOptions.SAMPLE_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.SampleIndexCommandOptions.SAMPLE_INDEX_COMMAND_DESCRIPTION;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.VariantSecondaryIndexCommandOptions.SECONDARY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.VariantSecondaryIndexRemoveCommandOptions.SECONDARY_INDEX_REMOVE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.FillGapsCommandOptions.FILL_GAPS_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.FillGapsCommandOptions.FILL_GAPS_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.FillMissingCommandOptions.FILL_MISSING_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.FillMissingCommandOptions.FILL_MISSING_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationDeleteCommandOptions.ANNOTATION_DELETE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationDeleteCommandOptions.ANNOTATION_DELETE_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationMetadataCommandOptions.ANNOTATION_METADATA_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationMetadataCommandOptions.ANNOTATION_METADATA_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationQueryCommandOptions.ANNOTATION_QUERY_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationQueryCommandOptions.ANNOTATION_QUERY_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationSaveCommandOptions.ANNOTATION_SAVE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationSaveCommandOptions.ANNOTATION_SAVE_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.VariantRemoveCommandOptions.VARIANT_REMOVE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.VariantRemoveCommandOptions.VARIANT_REMOVE_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils.*;

/**
 * Created by pfurio on 23/11/16.
 */
@Parameters(commandNames = {"variant"}, commandDescription = "Variant commands")
public class VariantCommandOptions {

    public final VariantIndexCommandOptions indexVariantCommandOptions;
    public final VariantRemoveCommandOptions variantRemoveCommandOptions;
    public final VariantSecondaryIndexCommandOptions variantSecondaryIndexCommandOptions;
    public final VariantSecondaryIndexRemoveCommandOptions variantSecondaryIndexRemoveCommandOptions;
//    public final QueryVariantCommandOptionsOld queryVariantCommandOptionsOld;
    public final VariantQueryCommandOptions queryVariantCommandOptions;
    public final VariantStatsCommandOptions statsVariantCommandOptions;
    public final VariantScoreIndexCommandOptions variantScoreIndexCommandOptions;
    public final VariantScoreRemoveCommandOptions variantScoreRemoveCommandOptions;
    public final SampleIndexCommandOptions sampleIndexCommandOptions;
    public final FamilyIndexCommandOptions familyIndexCommandOptions;
    public final VariantAnnotateCommandOptions annotateVariantCommandOptions;
    public final AnnotationSaveCommandOptions annotationSaveSnapshotCommandOptions;
    public final AnnotationDeleteCommandOptions annotationDeleteCommandOptions;
    public final AnnotationQueryCommandOptions annotationQueryCommandOptions;
    public final AnnotationMetadataCommandOptions annotationMetadataCommandOptions;
    public final FillGapsCommandOptions fillGapsVariantCommandOptions;
    public final FillMissingCommandOptions fillMissingCommandOptions;
    public final VariantExportStatsCommandOptions exportVariantStatsCommandOptions;
    public final VariantImportCommandOptions importVariantCommandOptions;
    public final VariantIbsCommandOptions ibsVariantCommandOptions;
    public final VariantSamplesFilterCommandOptions samplesFilterCommandOptions;
    public final VariantHistogramCommandOptions histogramCommandOptions;

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
        this.variantRemoveCommandOptions = new VariantRemoveCommandOptions();
        this.variantSecondaryIndexCommandOptions = new VariantSecondaryIndexCommandOptions();
        this.variantSecondaryIndexRemoveCommandOptions = new VariantSecondaryIndexRemoveCommandOptions();
//        this.queryVariantCommandOptionsOld = new QueryVariantCommandOptionsOld();
        this.queryVariantCommandOptions = new VariantQueryCommandOptions();
        this.statsVariantCommandOptions = new VariantStatsCommandOptions();
        this.variantScoreIndexCommandOptions = new VariantScoreIndexCommandOptions();
        this.variantScoreRemoveCommandOptions = new VariantScoreRemoveCommandOptions();
        this.sampleIndexCommandOptions = new SampleIndexCommandOptions();
        this.familyIndexCommandOptions = new FamilyIndexCommandOptions();
        this.annotateVariantCommandOptions = new VariantAnnotateCommandOptions();
        this.annotationSaveSnapshotCommandOptions = new AnnotationSaveCommandOptions();
        this.annotationDeleteCommandOptions = new AnnotationDeleteCommandOptions();
        this.annotationQueryCommandOptions = new AnnotationQueryCommandOptions();
        this.annotationMetadataCommandOptions = new AnnotationMetadataCommandOptions();
        this.fillGapsVariantCommandOptions = new FillGapsCommandOptions();
        this.fillMissingCommandOptions = new FillMissingCommandOptions();
        this.exportVariantStatsCommandOptions = new VariantExportStatsCommandOptions();
        this.importVariantCommandOptions = new VariantImportCommandOptions();
        this.ibsVariantCommandOptions = new VariantIbsCommandOptions();
        this.samplesFilterCommandOptions = new VariantSamplesFilterCommandOptions();
        this.histogramCommandOptions = new VariantHistogramCommandOptions();
    }

    @Parameters(commandNames = {"index"}, commandDescription = "Index variants file")
    public class VariantIndexCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public StorageVariantCommandOptions.GenericVariantIndexOptions genericVariantIndexOptions = new StorageVariantCommandOptions.GenericVariantIndexOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--file"}, description = "CSV of file ids to be indexed", required = true, arity = 1)
        public String fileId = null;

        @Parameter(names = {"--transformed-files"}, description = "CSV of paths corresponding to the location of the transformed files.",
                arity = 1)
        public String transformedPaths = null;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory outside catalog boundaries.", required = true, arity = 1)
        public String outdir = null;

        @Parameter(names = {"--path"}, description = "Path within catalog boundaries where the results will be stored. If not present, "
                + "transformed files will not be registered in catalog.", arity = 1)
        public String catalogPath = null;
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

        @Parameter(names = {"--cohort"}, description = VariantQueryParam.COHORT_DESCR, arity = 1)
        public String cohort;

        @Parameter(names = {"--overwrite"}, description = "Overwrite search index for all files and variants. Repeat operation for already processed variants.")
        public boolean overwrite;
    }

    @Parameters(commandNames = {SECONDARY_INDEX_REMOVE_COMMAND}, commandDescription = "Remove a secondary index from the search engine")
    public class VariantSecondaryIndexRemoveCommandOptions extends GeneralCliOptions.StudyOption {

        public static final String SECONDARY_INDEX_REMOVE_COMMAND = "secondary-index-remove";
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--sample"}, description = "Samples to remove. Needs to provide all the samples in the secondary index.",
                required = true, arity = 1)
        public String sample;
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

        @Parameter(names = {"--path"}, description = "Path within catalog boundaries where the results will be stored. If not present, "
                + "transformed files will not be registered in catalog.", arity = 1)
        public String catalogPath = null;


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
        public VariantAnnotatorFactory.AnnotationSource annotator = null;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        public boolean overwriteAnnotations;

        @Parameter(names = {"--resume"}, description = "Resume a previously failed indexation", arity = 0)
        public boolean resume;
    }

    @Parameters(commandNames = {VARIANT_REMOVE_COMMAND}, commandDescription = VARIANT_REMOVE_COMMAND_DESCRIPTION)
    public class VariantRemoveCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public StorageVariantCommandOptions.GenericVariantRemoveOptions genericVariantRemoveOptions = new StorageVariantCommandOptions.GenericVariantRemoveOptions();

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
    public class VariantQueryCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public StorageVariantCommandOptions.GenericVariantQueryOptions genericVariantQueryOptions = new StorageVariantCommandOptions.GenericVariantQueryOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"--sample-filter"}, description = SAMPLE_ANNOTATION_DESC)
        public String sampleFilter;

        // FIXME: This param should not be in the ANALYSIS command line!
        @Parameter(names = {"--mode"}, description = "Communication mode. grpc|rest|auto.")
        public String mode = "auto";

        @Parameter(names = {"-o", "--output"}, description = "Output file. [STDOUT]", arity = 1)
        public String output;

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
        public StorageVariantCommandOptions.GenericVariantStatsOptions genericVariantStatsOptions = new StorageVariantCommandOptions.GenericVariantStatsOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--cohort-ids"}, description = "Cohort Ids for the cohorts to be calculated.")
        public String cohortIds;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory outside catalog boundaries.", required = true, arity = 1)
        public String outdir = null;

        @Parameter(names = {"--path"}, description = "Path within catalog boundaries where the results will be stored. If not present, "
                + "transformed files will not be registered in catalog.", arity = 1)
        public String catalogPath = null;
    }

    @Parameters(commandNames = {VariantScoreIndexCommandOptions.SCORE_INDEX_COMMAND}, commandDescription = VariantScoreIndexCommandOptions.SCORE_INDEX_COMMAND_DESCRIPTION)
    public class VariantScoreIndexCommandOptions extends GeneralCliOptions.StudyOption {
        public static final String SCORE_INDEX_COMMAND = "score-index";
        public static final String SCORE_INDEX_COMMAND_DESCRIPTION = "Index a variant score in the database.";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--name"}, description = "Unique name of the score within the study", required = true)
        public String scoreName;

        @Parameter(names = {"--resume"}, description = "Resume a previously failed indexation", arity = 0)
        public boolean resume;

        @Parameter(names = {"--cohort1"}, description = "Cohort used to compute the score. "
                + "Use the cohort '" + StudyEntry.DEFAULT_COHORT + "' if all samples from the study where used to compute the score", required = true)
        public String cohort1;

        @Parameter(names = {"--cohort2"}, description = "Second cohort used to compute the score, typically to compare against the first cohort. "
                + "If only one cohort was used to compute the score, leave empty")
        public String cohort2;

        @Parameter(names = {"-i", "--input"}, description = "Input file to load", required = true)
        public String input;

        @Parameter(names = {"--input-columns"}, description = "Indicate which columns to load from the input file. "
                + "Provide the column position (starting in 0) for the column with the score with 'SCORE=n'. "
                + "Optionally, the PValue column with 'PVALUE=n'. "
                + "The, to indicate the variant associated with the score, provide either the columns ['CHROM', 'POS', 'REF', 'ALT'], "
                + "or the column 'VAR' containing a variant representation with format 'chr:start:ref:alt'. "
                + "e.g. 'CHROM=0,POS=1,REF=3,ALT=4,SCORE=5,PVALUE=6' or 'VAR=0,SCORE=1,PVALUE=2'", required = true)
        public String columns;
    }

    @Parameters(commandNames = {VariantScoreRemoveCommandOptions.SCORE_REMOVE_COMMAND}, commandDescription = VariantScoreRemoveCommandOptions.SCORE_REMOVE_COMMAND_DESCRIPTION)
    public class VariantScoreRemoveCommandOptions extends GeneralCliOptions.StudyOption {
        public static final String SCORE_REMOVE_COMMAND = "score-remove";
        public static final String SCORE_REMOVE_COMMAND_DESCRIPTION = "Remove a variant score from the database.";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--name"}, description = "Unique name of the score within the study", required = true)
        public String scoreName;

        @Parameter(names = {"--resume"}, description = "Resume a previously failed remove", arity = 0)
        public boolean resume;

        @Parameter(names = {"--force"}, description = "Force remove of partially indexed scores", arity = 0)
        public boolean force;
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

        @Parameter(names = {"--path"}, description = "Path within catalog boundaries where the results will be stored. If not present, "
                + "transformed files will not be registered in catalog.", arity = 1)
        public String catalogPath = null;

        @Parameter(names = {"--aggregated"}, description = "Select the type of aggregated VCF file: none, basic, EVS or ExAC", arity = 1)
        public Aggregation aggregated = Aggregation.NONE;

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF file")
        public String aggregationMappingFile;

        @Parameter(names = {"--resume"}, description = "Resume a previously failed stats calculation", arity = 0)
        public boolean resume;
    }

    @Parameters(commandNames = {"annotate"}, commandDescription = "Create and load variant annotations into the database")
    public class VariantAnnotateCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public StorageVariantCommandOptions.GenericVariantAnnotateOptions genericVariantAnnotateOptions = new StorageVariantCommandOptions.GenericVariantAnnotateOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project-id"}, description = "Project to annotate.", arity = 1)
        public String project;

//        @Parameter(names = {"-s", "--study-id"}, description = "Studies to annotate. Must be in the same database.", arity = 1)
//        public String study;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory outside catalog boundaries.", required = true, arity = 1)
        public String outdir;

        @Parameter(names = {"--path"}, description = "Path within catalog boundaries where the results will be stored. If not present, "
                + "transformed files will not be registered in catalog.", arity = 1)
        public String catalogPath;
    }

    @Parameters(commandNames = {ANNOTATION_SAVE_COMMAND}, commandDescription = ANNOTATION_SAVE_COMMAND_DESCRIPTION)
    public class AnnotationSaveCommandOptions extends StorageVariantCommandOptions.GenericAnnotationSaveCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;
    }

    @Parameters(commandNames = {ANNOTATION_DELETE_COMMAND}, commandDescription = ANNOTATION_DELETE_COMMAND_DESCRIPTION)
    public class AnnotationDeleteCommandOptions extends StorageVariantCommandOptions.GenericAnnotationDeleteCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;

    }

    @Parameters(commandNames = {ANNOTATION_QUERY_COMMAND}, commandDescription = ANNOTATION_QUERY_COMMAND_DESCRIPTION)
    public class AnnotationQueryCommandOptions extends StorageVariantCommandOptions.GenericAnnotationQueryCommandOptions {

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
    public class AnnotationMetadataCommandOptions extends StorageVariantCommandOptions.GenericAnnotationMetadataCommandOptions {

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

        @Parameter(names = {"--path"}, description = "Path within catalog boundaries where the results will be stored. If not present, "
                + "transformed files will not be registered in catalog.", arity = 1)
        public String catalogPath;

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
        public VariantAnnotatorFactory.AnnotationSource annotator;

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

    @Parameters(commandNames = {FILL_GAPS_COMMAND}, commandDescription = FILL_GAPS_COMMAND_DESCRIPTION)
    public class FillGapsCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public StorageVariantCommandOptions.GenericFillGapsOptions genericFillGapsOptions = new StorageVariantCommandOptions.GenericFillGapsOptions();
    }

    @Parameters(commandNames = {FILL_MISSING_COMMAND}, commandDescription = FILL_MISSING_COMMAND_DESCRIPTION)
    public class FillMissingCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public StorageVariantCommandOptions.GenericFillMissingCommandOptions fillMissingCommandOptions = new StorageVariantCommandOptions.GenericFillMissingCommandOptions();

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

        @Parameter(names = {"-o", "--output"}, description = "Output file. [STDOUT]", arity = 1)
        public String output;
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
        public StorageVariantCommandOptions.BasicVariantQueryOptions variantQueryOptions = new StorageVariantCommandOptions.BasicVariantQueryOptions();

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
        public StorageVariantCommandOptions.BasicVariantQueryOptions variantQueryOptions = new StorageVariantCommandOptions.BasicVariantQueryOptions();

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
}
