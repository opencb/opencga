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

package org.opencb.opencga.storage.app.cli.client.options;

import com.beust.jcommander.*;
import com.beust.jcommander.converters.CommaParameterSplitter;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.opencga.storage.app.cli.GeneralCliOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 22/01/17.
 */
@Parameters(commandNames = {"variant"}, commandDescription = "Variant management.")
public class StorageVariantCommandOptions {

    public final VariantIndexCommandOptions indexVariantsCommandOptions;
    public final VariantRemoveCommandOptions variantRemoveCommandOptions;
    public final VariantQueryCommandOptions variantQueryCommandOptions;
    public final ImportVariantsCommandOptions importVariantsCommandOptions;
    public final VariantAnnotateCommandOptions annotateVariantsCommandOptions;
    public final CreateAnnotationSnapshotCommandOptions createAnnotationSnapshotCommandOptions;
    public final DeleteAnnotationSnapshotCommandOptions deleteAnnotationSnapshotCommandOptions;
    public final QueryAnnotationCommandOptions queryAnnotationCommandOptions;
    public final VariantStatsCommandOptions statsVariantsCommandOptions;
    public final FillGapsCommandOptions fillGapsCommandOptions;
    public final FillMissingCommandOptions fillMissingCommandOptions;
    public final VariantExportCommandOptions exportVariantsCommandOptions;
    public final VariantSearchCommandOptions searchVariantsCommandOptions;

    public final JCommander jCommander;
    public final GeneralCliOptions.CommonOptions commonCommandOptions;
    public final GeneralCliOptions.IndexCommandOptions indexCommandOptions;
    public final GeneralCliOptions.QueryCommandOptions queryCommandOptions;

    public StorageVariantCommandOptions(GeneralCliOptions.CommonOptions commonOptions, GeneralCliOptions.IndexCommandOptions indexCommandOptions,
                                        GeneralCliOptions.QueryCommandOptions queryCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonOptions;
        this.indexCommandOptions  = indexCommandOptions;
        this.queryCommandOptions = queryCommandOptions;
        this.jCommander = jCommander;

        this.indexVariantsCommandOptions = new VariantIndexCommandOptions();
        this.variantRemoveCommandOptions = new VariantRemoveCommandOptions();
        this.variantQueryCommandOptions = new VariantQueryCommandOptions();
        this.importVariantsCommandOptions = new ImportVariantsCommandOptions();
        this.annotateVariantsCommandOptions = new VariantAnnotateCommandOptions();
        this.createAnnotationSnapshotCommandOptions = new CreateAnnotationSnapshotCommandOptions();
        this.deleteAnnotationSnapshotCommandOptions = new DeleteAnnotationSnapshotCommandOptions();
        this.queryAnnotationCommandOptions = new QueryAnnotationCommandOptions();
        this.statsVariantsCommandOptions = new VariantStatsCommandOptions();
        this.fillGapsCommandOptions = new FillGapsCommandOptions();
        this.fillMissingCommandOptions = new FillMissingCommandOptions();
        this.exportVariantsCommandOptions = new VariantExportCommandOptions();
        this.searchVariantsCommandOptions = new VariantSearchCommandOptions();
    }

    /**
     *  index: generic and specific options
     */
    public static class GenericVariantIndexOptions {

        @Parameter(names = {"--transform"}, description = "If present it only runs the transform stage, no load is executed")
        public boolean transform;

        @Parameter(names = {"--load"}, description = "If present only the load stage is executed, transformation is skipped")
        public boolean load;

        @Parameter(names = {"--merge"}, description = "Currently two levels of merge are supported: \"basic\" mode merge genotypes of the same variants while \"advanced\" merge multiallelic and overlapping variants.")
        public VariantStorageEngine.MergeMode merge = VariantStorageEngine.Options.MERGE_MODE.defaultValue();

        @Deprecated
        @Parameter(names = {"--include-stats"}, description = "Save statistics information available on the input file")
        public boolean includeStats;

        @Parameter(names = {"--exclude-genotypes"}, description = "Index excluding the genotype information")
        public boolean excludeGenotype;

        @Parameter(names = {"--include-extra-fields"}, description = "Index including other FORMAT fields." +
                " Use \"" + VariantQueryUtils.ALL + "\", \"" + VariantQueryUtils.NONE + "\", or CSV with the fields to load.")
        public String extraFields = VariantQueryUtils.ALL;

        @Parameter(names = {"--aggregated"}, description = "Select the type of aggregated VCF file: none, basic, EVS or ExAC", arity = 1)
        public Aggregation aggregated = Aggregation.NONE;

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF file")
        public String aggregationMappingFile;

        @Parameter(names = {"--gvcf"}, description = "The input file is in gvcf format")
        public boolean gvcf;

        @Parameter(names = {"--bgzip"}, description = "[PENDING] The input file is in bgzip format")
        public boolean bgzip;

        @Parameter(names = {"--calculate-stats"}, description = "Calculate indexed variants statistics after the load step")
        public boolean calculateStats;

        @Parameter(names = {"--annotate"}, description = "Annotate indexed variants after the load step")
        public boolean annotate;

        @Parameter(names = {"--index-search"}, description = "Indexed Solr search database")
        public boolean indexSearch;

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}", arity = 1)
        public VariantAnnotatorFactory.AnnotationSource annotator;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        public boolean overwriteAnnotations;

        @Parameter(names = {"--resume"}, description = "Resume a previously failed indexation")
        public boolean resume;

        @Parameter(names = {"--load-split-data"}, description = "Indicate that the variants from a sample (or group of samples) split into different files (by chromosome, by type, ...)")
        public boolean loadSplitData;

        @Parameter(names = {"--skip-post-load-check"}, description = "Do not execute post load checks over the database")
        public boolean skipPostLoadCheck;
    }

    @Parameters(commandNames = {"index"}, commandDescription = "Index variants file")
    public class VariantIndexCommandOptions extends GenericVariantIndexOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.IndexCommandOptions commonIndexOptions = indexCommandOptions;


        @Parameter(names = {"-s", "--study"}, description = "Full name of the study where the file is classified", arity = 1)
        public String study;

//        @Deprecated
//        @Parameter(names = {"-p", "--pedigree"}, description = "File containing pedigree information (in PED format, optional)", arity = 1)
//        public String pedigree;


        @Parameter(names = {"-t", "--study-type"}, description = "One of the following: FAMILY, TRIO, CONTROL, CASE, CASE_CONTROL, " +
                "PAIRED, PAIRED_TUMOR, COLLECTION, TIME_SERIES", arity = 1, hidden = true)
        public SampleSetType studyType = SampleSetType.CASE_CONTROL;

    }

    public static class GenericVariantRemoveOptions {

        @Parameter(names = {"--file"}, description = "CSV of files to be removed from storage. Type 'all' to remove the whole study",
                splitter = CommaParameterSplitter.class, required = true)
        public List<String> files = null;

        @Parameter(names = {"--resume"}, description = "Resume a previously failed indexation")
        public boolean resume;
    }

    @Parameters(commandNames = {VariantRemoveCommandOptions.VARIANT_REMOVE_COMMAND}, commandDescription = VariantRemoveCommandOptions.VARIANT_REMOVE_COMMAND_DESCRIPTION)
    public class VariantRemoveCommandOptions extends GenericVariantRemoveOptions {

        public static final String VARIANT_REMOVE_COMMAND = "remove";
        public static final String VARIANT_REMOVE_COMMAND_DESCRIPTION = "Remove variants from storage";

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study"}, description = "Full name of the study where the file is classified", arity = 1, required = true)
        public String study;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name to load the data", required = false, arity = 1)
        public String dbName;

    }

    /**
     *  query: basic, generic and specific options
     *
     * @see org.opencb.opencga.storage.app.cli.client.executors.VariantQueryCommandUtils#parseBasicVariantQuery
     */
    public static class BasicVariantQueryOptions {

        @Parameter(names = {"--id"}, description = VariantQueryParam.ID_DESCR, variableArity = true)
        public List<String> id;

        @Parameter(names = {"-r", "--region"}, description = VariantQueryParam.REGION_DESCR)
        public String region;

        @Parameter(names = {"--region-file"}, description = "GFF File with regions")
        public String regionFile;

        @Parameter(names = {"-g", "--gene"}, description = VariantQueryParam.GENE_DESCR)
        public String gene;

        @Parameter(names = {"-t", "--type"}, description = "Whether the variant is a: SNV, INDEL or SV")
        public String type;

        @Parameter(names = {"--ct", "--consequence-type"}, description = "Consequence type SO term list. example: SO:0000045,SO:0000046",
                arity = 1)
        public String consequenceType;

        @Parameter(names = {"-c", "--conservation"}, description = "Conservation score: {conservation_score}[<|>|<=|>=]{number} example: " +
                "phastCons>0.5,phylop<0.1", arity = 1)
        public String conservation;

        @Parameter(names = {"--ps", "--protein-substitution"}, description = "Filter by Sift or/and Polyphen scores, e.g. \"sift<0.2;polyphen<0.4\"", arity = 1)
        public String proteinSubstitution;

        @Parameter(names = {"--cadd"}, description = "Functional score: {functional_score}[<|>|<=|>=]{number} "
                + "e.g. cadd_scaled>5.2,cadd_raw<=0.3", arity = 1)
        public String functionalScore;

        @Parameter(names = {"--hpo"}, description = "List of HPO terms. e.g. \"HP:0000545,HP:0002812\"", arity = 1)
        public String hpo;

        @Parameter(names = {"--apf", "--alt-population-frequency"}, description = "Alternate Population Frequency: " +
                "{study}:{population}[<|>|<=|>=]{number}", arity = 1)
        public String populationFreqs;

        @Parameter(names = {"--maf", "--stats-maf"}, description = "Take a <STUDY>:<COHORT> and filter by Minor Allele Frequency, example: 1000g:all>0.4")
        public String maf;
    }

    /**
     * @see org.opencb.opencga.storage.app.cli.client.executors.VariantQueryCommandUtils#parseGenericVariantQuery
     */
    public static class GenericVariantQueryOptions extends BasicVariantQueryOptions {

        @Parameter(names = {"--group-by"}, description = "Group by gene, ensembl gene or consequence_type")
        public String groupBy;

        @Parameter(names = {"--rank"}, description = "Rank variants by gene, ensemblGene or consequence_type")
        public String rank;

//        @Parameter(names = {"-s", "--study"}, description = "A comma separated list of studies to be used as filter")
//        public String study;

        @Parameter(names = {"--gt", "--genotype"}, description = VariantQueryParam.GENOTYPE_DESCR, arity = 1)
        public String sampleGenotype;

        @Parameter(names = {"--sample"}, description = VariantQueryParam.SAMPLE_DESCR, arity = 1)
        public String samples;

        @Parameter(names = {"-f", "--file"}, description = "A comma separated list of files to be used as filter", arity = 1)
        public String file;

        @Parameter(names = {"--filter"}, description = VariantQueryParam.FILTER_DESCR, arity = 1)
        public String filter;

        @Parameter(names = {"--qual"}, description = VariantQueryParam.QUAL_DESCR, arity = 1)
        public String qual;

        @Parameter(names = {"--biotype"}, description = VariantQueryParam.ANNOT_BIOTYPE_DESCR, arity = 1)
        public String geneBiotype;

        @Parameter(names = {"--pmaf", "--population-maf"}, description = "Population minor allele frequency: " +
                "{study}:{population}[<|>|<=|>=]{number}", arity = 1)
        public String populationMaf;

        @Parameter(names = {"--transcript-flag"}, description = "List of transcript annotation flags. e.g. CCDS,basic,cds_end_NF, mRNA_end_NF,cds_start_NF,mRNA_start_NF,seleno", arity = 1)
        public String flags;

        // TODO Jacobo please implement this ASAP
        @Parameter(names = {"--gene-trait"}, description = "[PENDING] List of gene trait association IDs or names. e.g. \"umls:C0007222,Cardiovascular Diseases\"", arity = 1)
        public String geneTrait;

        @Deprecated
        @Parameter(names = {"--gene-trait-id"}, description = "[DEPRECATED] List of gene trait association names. e.g. \"Cardiovascular Diseases\"", arity = 1)
        public String geneTraitId;

        @Deprecated
        @Parameter(names = {"--gene-trait-name"}, description = "[DEPRECATED] List of gene trait association id. e.g. \"umls:C0007222,OMIM:269600\"", arity = 1)
        public String geneTraitName;

        @Parameter(names = {"--go", "--gene-ontology"}, description = "List of Gene Ontology (GO) accessions or names. e.g. \"GO:0002020\"", arity = 1)
        public String go;

//        @Parameter(names = {"--expression", "--tissue"}, description = "List of tissues of interest. e.g. \"tongue\"", arity = 1)
//        public String expression;

        @Parameter(names = {"--protein-keywords"}, description = "List of Uniprot protein keywords", arity = 1)
        public String proteinKeywords;

        @Parameter(names = {"--drug"}, description = "List of drug names", arity = 1)
        public String drugs;

        @Deprecated
        @Parameter(names = {"--gwas"}, description = "[DEPRECATED]", arity = 1, hidden = true)
        public String gwas;

        @Parameter(names = {"--cosmic"}, description = VariantQueryParam.ANNOT_COSMIC_DESCR, arity = 1)
        public String cosmic;

        @Parameter(names = {"--clinvar"}, description = VariantQueryParam.ANNOT_CLINVAR_DESCR, arity = 1)
        public String clinvar;

        @Deprecated
        @Parameter(names = {"--stats"}, description = "[DEPRECATED]", hidden = true)
        public String stats;

        @Parameter(names = {"--mgf", "--stats-mgf"}, description = "Take a <STUDY>:<COHORT> and filter by Minor Genotype Frequency, example: 1000g:all<=0.4")
        public String mgf;

        @Parameter(names = {"--stats-missing-allele"}, description = "Take a <STUDY>:<COHORT> and filter by number of missing alleles, example: 1000g:all=5")
        public String missingAlleleCount;

        @Parameter(names = {"--stats-missing-genotype"}, description = "Take a <STUDY>:<COHORT> and filter by number of missing genotypes, " +
                "example: 1000g:all!=0")
        public String missingGenotypeCount;


        @Parameter(names = {"--dominant"}, description = "[PENDING] Take a family in the form of: FATHER,MOTHER,CHILD and specifies if is" +
                " affected or not to filter by dominant segregation, example: 1000g:NA001:aff,1000g:NA002:unaff,1000g:NA003:aff",
                required = false)
        public String dominant;

        @Parameter(names = {"--recessive"}, description = "[PENDING] Take a family in the form of: FATHER,MOTHER,CHILD and specifies if " +
                "is affected or not to filter by recessive segregation, example: 1000g:NA001:aff,1000g:NA002:unaff,1000g:NA003:aff",
                required = false)
        public String recessive;

        @Parameter(names = {"--ch", "--compound-heterozygous"}, description = "[PENDING] Take a family in the form of: FATHER,MOTHER," +
                "CHILD and specifies if is affected or not to filter by compound heterozygous, example: 1000g:NA001:aff," +
                "1000g:NA002:unaff,1000g:NA003:aff")
        public String compoundHeterozygous;


        @Parameter(names = {"--output-study"}, description = "A comma separated list of studies to be returned")
        public String returnStudy;

        @Parameter(names = {"--output-file"}, description = "A comma separated list of files from the SAME study to be returned")
        public String returnFile;

        @Parameter(names = {"--include-format"}, description = VariantQueryParam.INCLUDE_FORMAT_DESCR)
        public String includeFormat;

        @Parameter(names = {"--include-genotype"}, description = VariantQueryParam.INCLUDE_GENOTYPE_DESCR)
        public boolean includeGenotype;

        @Parameter(names = {"--output-sample"}, description = "A comma separated list of samples from the SAME study to be returned")
        public String returnSample;

        @Parameter(names = {"--annotations", "--output-vcf-info"}, description = "Set variant annotation to return in the INFO column. " +
                "Accepted values include 'all', 'default' aor a comma-separated list such as 'gene,biotype,consequenceType'", arity = 1)
        public String annotations;

        @Parameter(names = {"--output-unknown-genotype"}, description = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]")
        public String unknownGenotype = "./.";

        @Parameter(names = {"--output-histogram"}, description = "Calculate histogram. Requires --region.")
        public boolean histogram;

        @Parameter(names = {"--histogram-interval"}, description = "Histogram interval size. Default:2000", arity = 1)
        public int interval;

        @Deprecated
        @Parameter(names = {"--annot-xref"}, description = "[DEPRECATED] XRef", arity = 1)
        public String annotXref;

        @Parameter(names = {"--sample-metadata"}, description = "Returns the samples metadata group by study. Sample names will appear in the same order as their corresponding genotypes.")
        public boolean samplesMetadata;

        @Parameter(names = {"--summary"}, description = "Fast fetch of main variant parameters")
        public boolean summary;

        @Parameter(names = {"--sort"}, description = "Sort the output elements.")
        public boolean sort;
    }

    @Parameters(commandNames = {"query"}, commandDescription = "Search over indexed variants")
    public class VariantQueryCommandOptions extends GenericVariantQueryOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.QueryCommandOptions commonQueryOptions = queryCommandOptions;

        @Parameter(names = {"-s", "--study"}, description = "A comma separated list of studies to be used as filter")
        public String study;

//        @Parameter(names = {"-o", "--output"}, description = "Output file. [STDOUT]", arity = 1)
//        public String output;
//
//        @Parameter(names = {"-d", "--database"}, description = "DataBase name", arity = 1)
//        public String dbName;
//
//        @Parameter(names = {"-i", "--include"}, description = "", arity = 1)
//        public String include;
//
//        @Parameter(names = {"-e", "--exclude"}, description = "", arity = 1)
//        public String exclude;
//
//        @Parameter(names = {"--skip"}, description = "Skip some number of elements.", arity = 1)
//        public int skip;
//
//        @Parameter(names = {"--limit"}, description = "Limit the number of returned elements.", arity = 1)
//        public int limit;
//
//        @Parameter(names = {"--count"}, description = "Count results. Do not return elements.", arity = 0)
//        public boolean count;

        @Parameter(names = {"--of", "--output-format"}, description = "Output format: vcf, vcf.gz, json or json.gz", arity = 1)
        public String outputFormat = "vcf";

    }

    /**
     *  import: specific options
     */
    @Parameters(commandNames = {"import"}, commandDescription = "Import a variants dataset into an empty database")
    public class ImportVariantsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "File to import in the selected backend", required = true)
        public String input;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name to load the data", arity = 1)
        public String dbName;

    }

    /**
     *  annotate: generic and specific options
     */
    public static class GenericVariantAnnotateOptions {

        @Parameter(names = {"--create"}, description = "Run only the creation of the annotations to a file (specified by --output-filename)")
        public boolean create;

        @Parameter(names = {"--load"}, description = "Run only the load of the annotations into the DB from FILE")
        public String load;

        @Parameter(names = {"--custom-name"}, description = "Provide a name to the custom annotation")
        public String customAnnotationKey = null;

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
        public VariantAnnotatorFactory.AnnotationSource annotator;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        public boolean overwriteAnnotations;

        @Parameter(names = {"--output-filename"}, description = "Output file name. Default: dbName", arity = 1)
        public String fileName;

        @Parameter(names = {"--filter-region"}, description = "Comma separated region filters", splitter = CommaParameterSplitter.class)
        public String filterRegion;

        @Deprecated
        @Parameter(names = {"--filter-gene"}, description = "Comma separated gene filters", splitter = CommaParameterSplitter.class)
        public String filterGene;

        @Parameter(names = {"--filter-annot-consequence-type"}, description = "Comma separated annotation consequence type filters",
                splitter = CommaParameterSplitter.class)
        public List filterAnnotConsequenceType = null; // TODO will receive CSV, only available when create annotations
    }

    @Parameters(commandNames = {"annotate"}, commandDescription = "Create and load variant annotations into the database")
    public class VariantAnnotateCommandOptions extends GenericVariantAnnotateOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--species"}, description = "Species. Default hsapiens", arity = 1)
        public String species = "hsapiens";

        @Parameter(names = {"--assembly"}, description = "Assembly. Default GRch37", arity = 1)
        public String assembly = "GRCh37";

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = true, arity = 1)
        public String dbName;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1)
        public String outdir;
    }

    public static class GenericCreateAnnotationSnapshotCommandOptions {
        @Parameter(names = {"--name"}, description = "Annotation snapshot name", required = true, arity = 1)
        public String name;
    }

    @Parameters(commandNames = {CreateAnnotationSnapshotCommandOptions.COPY_ANNOTATION_COMMAND}, commandDescription = CreateAnnotationSnapshotCommandOptions.COPY_ANNOTATION_COMMAND_DESCRIPTION)
    public class CreateAnnotationSnapshotCommandOptions extends GenericCreateAnnotationSnapshotCommandOptions {
        public static final String COPY_ANNOTATION_COMMAND = "copy-annotation";
        public static final String COPY_ANNOTATION_COMMAND_DESCRIPTION = "Creates a snapshot of the current variant annotation at the database.";

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = true, arity = 1)
        public String dbName;

    }

    public static class GenericDeleteAnnotationSnapshotCommandOptions {

        @Parameter(names = {"--name"}, description = "Annotation snapshot name", required = true, arity = 1)
        public String name;
    }

    @Parameters(commandNames = {DeleteAnnotationSnapshotCommandOptions.DELETE_ANNOTATION_COMMAND}, commandDescription = DeleteAnnotationSnapshotCommandOptions.DELETE_ANNOTATION_COMMAND_DESCRIPTION)
    public class DeleteAnnotationSnapshotCommandOptions extends GenericDeleteAnnotationSnapshotCommandOptions {
        public static final String DELETE_ANNOTATION_COMMAND = "delete-annotation";
        public static final String DELETE_ANNOTATION_COMMAND_DESCRIPTION = "Deletes a variant annotation snapshot.";

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = true, arity = 1)
        public String dbName;

    }

    public static class GenericQueryAnnotationCommandOptions {

        @Parameter(names = {"--name"}, description = "Annotation snapshot name", required = true, arity = 1)
        public String name = VariantAnnotationManager.LATEST;

        @Parameter(names = {"--id"}, description = VariantQueryParam.ID_DESCR, variableArity = true)
        public List<String> id;

        @Parameter(names = {"-r", "--region"}, description = VariantQueryParam.REGION_DESCR)
        public String region;

    }

    @Parameters(commandNames = {QueryAnnotationCommandOptions.QUERY_ANNOTATION_COMMAND}, commandDescription = QueryAnnotationCommandOptions.QUERY_ANNOTATION_COMMAND_DESCRIPTION)
    public class QueryAnnotationCommandOptions extends GenericQueryAnnotationCommandOptions {
        public static final String QUERY_ANNOTATION_COMMAND = "annotation";
        public static final String QUERY_ANNOTATION_COMMAND_DESCRIPTION = "";

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.DataModelOptions dataModelOptions = new GeneralCliOptions.DataModelOptions();

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = true, arity = 1)
        public String dbName;

        @Parameter(names = {"--skip"}, description = "Skip some number of elements.", required = false, arity = 1)
        public int skip;

        @Parameter(names = {"--limit"}, description = "Limit the number of returned elements.", required = false, arity = 1)
        public int limit;
    }

    /**
     *  annotate: generic and specific options
     */
    public static class GenericFillGapsOptions {

        @Parameter(names = {"--samples"}, description = "Samples within the same study to fill", required = true)
        public List<String> samples;

        @Parameter(names = {"--resume"}, description = "Resume a previously failed operation")
        public boolean resume;

//        @Parameter(names = {"--exclude-hom-ref"}, description = "Do not fill gaps of samples with HOM-REF genotype (0/0)", arity = 0)
//        public boolean excludeHomRef;
    }

    @Parameters(commandNames = {FillGapsCommandOptions.FILL_GAPS_COMMAND}, commandDescription = FillGapsCommandOptions.FILL_GAPS_COMMAND_DESCRIPTION)
    public class FillGapsCommandOptions extends GenericFillGapsOptions {

        public static final String FILL_GAPS_COMMAND = "fill-gaps";
        public static final String FILL_GAPS_COMMAND_DESCRIPTION = "Find variants where not all the samples are present, and fill the empty values.";

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study"}, description = "Study", arity = 1)
        public String study;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = true, arity = 1)
        public String dbName;
    }

    @Parameters(commandNames = {FillMissingCommandOptions.FILL_MISSING_COMMAND}, commandDescription = FillMissingCommandOptions.FILL_MISSING_COMMAND_DESCRIPTION)
    public class FillMissingCommandOptions extends GenericFillMissingCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study"}, description = "Study", arity = 1)
        public String study;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = true, arity = 1)
        public String dbName;
    }

    public static class GenericFillMissingCommandOptions {
        public static final String FILL_MISSING_COMMAND = "fill-missing";
        public static final String FILL_MISSING_COMMAND_DESCRIPTION = "Find variants where not all the samples are present, and fill the empty values, excluding HOM-REF (0/0) values.";

        @Parameter(names = {"--resume"}, description = "Resume a previously failed operation")
        public boolean resume;

        @Parameter(names = {"--overwrite"}, description = "Overwrite gaps for all files and variants. Repeat operation for already processed variants.")
        public boolean overwrite;
    }

    /**
     *  benchmark: specific options
     */
    @Parameters(commandNames = {"benchmark"}, commandDescription = "[PENDING] Benchmark load and fetch variants with different databases")
    public class BenchmarkCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;


        @Parameter(names = {"--num-repetition"}, description = "Number of repetition", arity = 1)
        public int repetition = 3;

        @Parameter(names = {"--load"}, description = "File name with absolute path", arity = 1)
        public String load;

        @Parameter(names = {"--queries"}, description = "Queries to fetch the data from tables", arity = 1)
        public String queries;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", arity = 1)
        public String database;

        @Parameter(names = {"-t", "--table"}, description = "Benchmark variants", arity = 1)
        public String table;

        @Parameter(names = {"--host"}, description = "DataBase name", arity = 1)
        public String host;

        @Parameter(names = {"--concurrency"}, description = "Number of threads to run in parallel", arity = 1)
        public int concurrency = 1;

    }

    /**
     *  stats: generic and specific options
     */
    public static class GenericVariantStatsOptions {

        @Parameter(names = {"--create"}, description = "Run only the creation of the stats to a file")
        public boolean create = false;

        @Parameter(names = {"--load"}, description = "Load the stats from an already existing FILE directly into the database. FILE is a " +
                "prefix with structure <INPUT_FILENAME>.<TIME>")
        public boolean load = false;

        @Parameter(names = {"--overwrite-stats"}, description = "Overwrite stats in variants already present")
        public boolean overwriteStats = false;

        @Parameter(names = {"--region"}, description = "[PENDING] Region to calculate.")
        public String region;

        @Parameter(names = {"--update-stats"}, description = "Calculate stats just for missing positions. Assumes that existing stats are" +
                " correct")
        public boolean updateStats = false;

//        @Parameter(names = {"-s", "--study-id"}, description = "Unique ID for the study where the file is classified", required = true,
//                arity = 1)
//        public String studyId;

//        @Parameter(names = {"-f", "--file"}, description = "Calculate stats only for the selected file", arity = 1)
//        public String file;

        @Parameter(names = {"--output-filename"}, description = "Output file name. Default: database name", arity = 1)
        public String fileName;

        @Parameter(names = {"--aggregated"}, description = "Select the type of aggregated VCF file: none, basic, EVS or ExAC", arity = 1)
        public Aggregation aggregated = Aggregation.NONE;

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF file")
        public String aggregationMappingFile;

        @Parameter(names = {"--resume"}, description = "Resume a previously failed stats calculation", arity = 0)
        public boolean resume;
    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Create and load stats into a database.")
    public class VariantStatsCommandOptions extends GenericVariantStatsOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study"}, description = "Unique ID for the study where the file is classified", required = true,
                arity = 1)
        public String study;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", arity = 1)
        public String dbName;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", arity = 1)
        public String outdir = ".";

        @DynamicParameter(names = {"--cohort"}, description = "Cohort definition with the schema -> <cohort>:<sample>" +
                "(,<sample>)* ", descriptionKey = "CohortName", assignment = ":")
        public Map<String, String> cohort = new HashMap<>();
    }

    /**
     * export: generic and specific options
     */

    public static class GenericVariantExportOptions {

        @Parameter(names = {"--output-filename"}, description = "Output filename.", arity = 1)
        public String outFilename = ".";

//        @Parameter(names = {"--region"}, description = "Variant region to export.")
//        public String region;

//        @Parameter(names = {"--study-configuration-file"}, description = "File with the study configuration. org.opencb.opencga.storage" +
//                ".core.StudyConfiguration", arity = 1)
//        public String studyConfigurationFile;
    }


    @Parameters(commandNames = {"export"}, commandDescription = "Export variants into a VCF file.")
    public class VariantExportCommandOptions extends GenericVariantExportOptions {

//        @ParametersDelegate
//        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public VariantQueryCommandOptions queryOptions = new VariantQueryCommandOptions();

//
//        @Parameter(names = {"-d", "--database"}, description = "DataBase name", arity = 1)
//        public String dbName;
    }

    /**
     * export: specific options
     */

    public static class GenericVariantSearchOptions extends BasicVariantQueryOptions {

        // TODO: both clinvar and cosmic should be moved to basic variant query options
        @Parameter(names = {"---clinvar"}, description = "List of ClinVar accessions or traits.", arity = 1)
        public String clinvar;

        @Parameter(names = {"---cosmic"}, description = "List of COSMIC mutation IDs, primary histologies"
                + " or histology subtypes.", arity = 1)
        public String cosmic;

        @Parameter(names = {"--facet"}, description = "Facet search.", arity = 1)
        public String facet;

        @Deprecated
        @Parameter(names = {"--facetRange"}, description = "Facet range (DEPRECATED)", arity = 1)
        public String facetRange;
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Solr support.")
    public class VariantSearchCommandOptions extends GenericVariantSearchOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--index"}, description = "Index a file into core/collection Solr.", arity = 0)
        public boolean index;

        @Parameter(names = {"-i", "--input"}, description = "Path to the file to index. Valid formats: AVRO and JSON.", arity = 1)
        public String inputFilename;

//        @Parameter(names = {"-f", "--file-id"}, description = "Calculate stats only for the selected file", arity = 1)
//        public String fileId;
//
//        @Parameter(names = {"-s", "--study-id"}, description = "Unique ID for the study where the file is classified", required = true,
//                arity = 1)
//        public String studyId;
//

//        @Parameter(names = {"--mode"}, description = "Search mode. Valid values: core, collection.", arity = 1)
//        public String mode = "core";

        @Parameter(names = {"--create"}, description = "Create a new core/collection.", arity = 0)
        public boolean create;

        @Parameter(names = {"--solr-url"}, description = "Url to Solr server, e.g.: http://localhost:8983/solr/", arity = 1)
        public String solrUrl;

        @Parameter(names = {"--solr-config"}, description = "Solr configuration name.", arity = 1)
        public String solrConfig;

//        @Parameter(names = {"--solr-num-shards"}, description = "Number of Solr collection shards (only for a Solr cluster mode).", arity = 1)
//        public int numShards = 2;
//
//        @Parameter(names = {"--solr-num-replicas"}, description = "Number of Solr collection replicas (only for a Solr cluster mode).", arity = 1)
//        public int numReplicas = 2;

        @Parameter(names = {"-d", "--database"}, description = "Name of the target core ore collection.", arity = 1)
        public String dbName;
    }
}
