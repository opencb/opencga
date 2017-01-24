/*
 * Copyright 2015-2016 OpenCB
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
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.opencga.storage.app.cli.GeneralCliOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 22/01/17.
 */
public class StorageVariantCommandOptions {

    public IndexVariantsCommandOptions indexVariantsCommandOptions;
    public VariantQueryCommandOptions variantQueryCommandOptions;
    public ImportVariantsCommandOptions importVariantsCommandOptions;
    public AnnotateVariantsCommandOptions annotateVariantsCommandOptions;
    public StatsVariantsCommandOptions statsVariantsCommandOptions;

    public JCommander jCommander;
    public GeneralCliOptions.CommonOptions commonCommandOptions;
    public GeneralCliOptions.IndexCommandOptions indexCommandOptions;
    public GeneralCliOptions.QueryCommandOptions queryCommandOptions;

    public StorageVariantCommandOptions(GeneralCliOptions.CommonOptions commonOptions, GeneralCliOptions.IndexCommandOptions indexCommandOptions,
                                        GeneralCliOptions.QueryCommandOptions queryCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonOptions;
        this.indexCommandOptions  = indexCommandOptions;
        this.queryCommandOptions = queryCommandOptions;
        this.jCommander = jCommander;

        this.indexVariantsCommandOptions = new IndexVariantsCommandOptions();
        this.variantQueryCommandOptions = new VariantQueryCommandOptions();
        this.importVariantsCommandOptions = new ImportVariantsCommandOptions();
        this.annotateVariantsCommandOptions = new AnnotateVariantsCommandOptions();
        this.statsVariantsCommandOptions = new StatsVariantsCommandOptions();
    }


    @Parameters(commandNames = {"index"}, commandDescription = "Index variants file")
    public class IndexVariantsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.IndexCommandOptions commonIndexOptions = indexCommandOptions;


        @Parameter(names = {"--study-name"}, description = "Full name of the study where the file is classified", required = false, arity = 1)
        public String study;

        @Parameter(names = {"-s", "--study-id"}, description = "Unique ID for the study where the file is classified", required = false, arity = 1)
        public String studyId = VariantStorageEngine.Options.STUDY_ID.defaultValue().toString();

        @Parameter(names = {"--file-id"}, description = "Unique ID for the file", required = false, arity = 1)
        public String fileId = VariantStorageEngine.Options.FILE_ID.defaultValue().toString();

        @Parameter(names = {"-p", "--pedigree"}, description = "File containing pedigree information (in PED format, optional)", arity = 1)
        public String pedigree;

        @Parameter(names = {"--sample-ids"}, description = "CSV list of sampleIds. <sampleName>:<sampleId>[,<sampleName>:<sampleId>]*")
        public List<String> sampleIds;

        @Deprecated
        @Parameter(names = {"--include-stats"}, description = "Save statistics information available on the input file")
        public boolean includeStats = false;

        @Parameter(names = {"--include-genotypes"}, description = "Index including the genotypes")
        public boolean includeGenotype = false;

        @Parameter(names = {"--include-extra-fields"}, description = "Index including other genotype fields [CSV]")
        public String extraFields;

        @Deprecated
        @Parameter(names = {"--compress-genotypes"}, description = "[DEPRECATED]")
        public boolean compressGenotypes = false;

        @Deprecated
        @Parameter(names = {"--include-src"}, description = "Store also the source vcf row of each variant")
        public boolean includeSrc = false;

        @Parameter(names = {"--aggregated"}, description = "Select the type of aggregated VCF file: none, basic, EVS or ExAC", arity = 1)
        public VariantSource.Aggregation aggregated = VariantSource.Aggregation.NONE;

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF " +
                "file")
        public String aggregationMappingFile = null;

        @Parameter(names = {"-t", "--study-type"}, description = "One of the following: FAMILY, TRIO, CONTROL, CASE, CASE_CONTROL, " +
                "PAIRED, PAIRED_TUMOR, COLLECTION, TIME_SERIES", arity = 1)
        public VariantStudy.StudyType studyType = VariantStudy.StudyType.CASE_CONTROL;

        @Parameter(names = {"--gvcf"}, description = "[PENDING] The input file is in gvcf format")
        public boolean gvcf;

        @Parameter(names = {"--bgzip"}, description = "[PENDING] The input file is in bgzip format")
        public boolean bgzip;

        @Parameter(names = {"--calculate-stats"}, description = "Calculate indexed variants statistics after the load step")
        public boolean calculateStats;

        @Parameter(names = {"--annotate"}, description = "Annotate indexed variants after the load step")
        public boolean annotate;

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
        public VariantAnnotatorFactory.AnnotationSource annotator = null;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        public boolean overwriteAnnotations;

        @Deprecated
        @Parameter(names = {"--annotator-config"}, description = "Path to the file with the configuration of the annotator")
        public String annotatorConfigFile;

        @Parameter(names = {"--resume"}, description = "Resume a previously failed indexation", arity = 0)
        public boolean resume;
    }

    public static class GenericVariantQueryOptions {

        @Parameter(names = {"--id"}, description = "CSV list of variant ids")
        public String id;

        @Parameter(names = {"-r", "--region"}, description = "CSV list of regions: {chr}[:{start}-{end}], eg.: 2,3:1000000-2000000")
        public String region;

        @Parameter(names = {"--region-file"}, description = "GFF File with regions")
        public String regionFile;

        @Parameter(names = {"-g", "--gene"}, description = "CSV list of genes")
        public String gene;

        @Parameter(names = {"--group-by"}, description = "Group by gene, ensembl gene or consequence_type", required = false)
        public String groupBy;

        @Parameter(names = {"--rank"}, description = "Rank variants by gene, ensemblGene or consequence_type", required = false)
        public String rank;

        @Parameter(names = {"-s", "--study"}, description = "A comma separated list of studies to be used as filter", required = false)
        public String study;

        @Parameter(names = {"--gt", "--genotype"}, description = "A comma separated list of samples from the SAME study, example: " +
                "NA0001:0/0,0/1;NA0002:0/1", required = false, arity = 1)
        public String sampleGenotype;

        @Parameter(names = {"-f", "--file"}, description = "A comma separated list of files to be used as filter", required = false, arity = 1)
        public String file;

        @Parameter(names = {"-t", "--type"}, description = "Whether the variant is a: SNV, INDEL or SV", required = false)
        public String type;

        @Parameter(names = {"--annotations"}, description = "Set variant annotation to return in the INFO column. " +
                "Accepted values include 'all', 'default' aor a comma-separated list such as 'gene,biotype,consequenceType'", required = false, arity = 1)
        public String annotations;

        @Parameter(names = {"--ct", "--consequence-type"}, description = "Consequence type SO term list. example: SO:0000045,SO:0000046",
                required = false, arity = 1)
        public String consequenceType;

        @Parameter(names = {"--gene-biotype"}, description = "Biotype CSV", required = false, arity = 1)
        public String geneBiotype;

        @Parameter(names = {"--pf", "--population-frequency"}, description = "Alternate Population Frequency: " +
                "{study}:{population}[<|>|<=|>=]{number}", required = false, arity = 1)
        public String populationFreqs;

        @Parameter(names = {"--pmaf", "--population-maf"}, description = "Population minor allele frequency: " +
                "{study}:{population}[<|>|<=|>=]{number}", required = false, arity = 1)
        public String populationMaf;

        @Parameter(names = {"-c", "--conservation"}, description = "Conservation score: {conservation_score}[<|>|<=|>=]{number} example: " +
                "phastCons>0.5,phylop<0.1", required = false, arity = 1)
        public String conservation;

        @Parameter(names = {"--transcript-flag"}, description = "List of transcript annotation flags. e.g. CCDS,basic,cds_end_NF, mRNA_end_NF,cds_start_NF,mRNA_start_NF,seleno", required = false, arity = 1)
        public String flags;

        // TODO Jacobo please implement this ASAP
        @Parameter(names = {"--gene-trait"}, description = "List of gene trait association IDs or names. e.g. \"umls:C0007222,Cardiovascular Diseases\"", arity = 1)
        public String geneTrait;

        @Deprecated
        @Parameter(names = {"--gene-trait-id"}, description = "List of gene trait association names. e.g. \"Cardiovascular Diseases\"", required = false, arity = 1)
        public String geneTraitId;

        @Deprecated
        @Parameter(names = {"--gene-trait-name"}, description = "List of gene trait association id. e.g. \"umls:C0007222,OMIM:269600\"", required = false, arity = 1)
        public String geneTraitName;

        @Parameter(names = {"--hpo"}, description = "List of HPO terms. e.g. \"HP:0000545,HP:0002812\"", required = false, arity = 1)
        public String hpo;

        @Parameter(names = {"--go", "--gene-ontology"}, description = "List of Gene Ontology (GO) accessions or names. e.g. \"GO:0002020\"", required = false, arity = 1)
        public String go;

        @Parameter(names = {"--expression", "--tissue"}, description = "List of tissues of interest. e.g. \"tongue\"", required = false, arity = 1)
        public String expression;

        @Parameter(names = {"--protein-keywords"}, description = "List of protein variant annotation keywords", required = false, arity = 1)
        public String proteinKeywords;

        @Parameter(names = {"--drug"}, description = "List of drug names", required = false, arity = 1)
        public String drugs;

        @Parameter(names = {"--ps", "--protein-substitution"}, description = "", required = false, arity = 1)
        public String proteinSubstitution;

        @Deprecated
        @Parameter(names = {"--gwas"}, description = "", required = false, arity = 1)
        public String gwas;

        @Deprecated
        @Parameter(names = {"--cosmic"}, description = "", required = false, arity = 1)
        public String cosmic;

        @Parameter(names = {"--clinvar"}, description = "", required = false, arity = 1)
        public String clinvar;

        @Deprecated
        @Parameter(names = {"--stats"}, description = " [CSV]", required = false)
        public String stats;

        @Parameter(names = {"--maf", "--stats-maf"}, description = "Take a <STUDY>:<COHORT> and filter by Minor Allele Frequency, example: 1000g:all>0.4", required = false)
        public String maf;

        @Parameter(names = {"--mgf", "--stats-mgf"}, description = "Take a <STUDY>:<COHORT> and filter by Minor Genotype Frequency, example: " +
                "1000g:all<=0.4", required = false)
        public String mgf;

        @Parameter(names = {"--stats-missing-allele"}, description = "Take a <STUDY>:<COHORT> and filter by number of missing alleles, example:" +
                " 1000g:all=5", required = false)
        public String missingAlleleCount;

        @Parameter(names = {"--stats-missing-genotype"}, description = "Take a <STUDY>:<COHORT> and filter by number of missing genotypes, " +
                "example: 1000g:all!=0", required = false)
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
                "1000g:NA002:unaff,1000g:NA003:aff", required = false)
        public String compoundHeterozygous;


        @Parameter(names = {"--output-study"}, description = "A comma separated list of studies to be returned", required = false)
        public String returnStudy;

        @Parameter(names = {"--return-sample"}, description = "A comma separated list of samples from the SAME study to be returned", required = false)
        public String returnSample;

        @Parameter(names = {"--unknown-genotype"}, description = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]", required = false)
        public String unknownGenotype = "./.";

        @Parameter(names = {"--histogram"}, description = "Calculate histogram. Requires one region.", arity = 0)
        public boolean histogram;

        @Parameter(names = {"--interval"}, description = "Histogram interval size. Default:2000", arity = 1)
        public String interval;

        @Parameter(names = {"--cadd", "--annot-functional-score"}, description = "Functional score: {functional_score}[<|>|<=|>=]{number} "
                + "e.g. cadd_scaled>5.2 , cadd_raw<=0.3", arity = 1)
        public String functionalScore;

        @Parameter(names = {"--annot-xref"}, description = "XRef", arity = 1)
        public String annot_xref;

        @Parameter(names = {"--samples-metadata"}, description = "Returns the samples metadata group by studyId, instead of the variants",
                arity = 0)
        public boolean samplesMetadata;

    }

    @Parameters(commandNames = {"query"}, commandDescription = "Search over indexed variants")
    public class VariantQueryCommandOptions extends GenericVariantQueryOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

//        @ParametersDelegate
//        public OptionsParser.QueryCommandOptions commonQueryOptions = queryCommandOptions;


        @Parameter(names = {"-o", "--output"}, description = "Output file. [STDOUT]", required = false, arity = 1)
        public String output;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = false, arity = 1)
        public String dbName;

        @Parameter(names = {"-i", "--include"}, description = "", required = false, arity = 1)
        public String include;

        @Parameter(names = {"-e", "--exclude"}, description = "", required = false, arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Skip some number of elements.", required = false, arity = 1)
        public int skip;

        @Parameter(names = {"--limit"}, description = "Limit the number of returned elements.", required = false, arity = 1)
        public int limit;

        @Parameter(names = {"--count"}, description = "Count results. Do not return elements.", required = false, arity = 0)
        public boolean count;


        @Parameter(names = {"--mode"}, description = "Communication mode. grpc|rest|auto.")
        public String mode = "auto";

        @Parameter(names = {"--of", "--output-format"}, description = "Output format: vcf, vcf.gz, json or json.gz", required = false, arity = 1)
        public String outputFormat = "vcf";

    }

    @Parameters(commandNames = {"import"}, commandDescription = "Import a variants dataset into an empty database")
    public class ImportVariantsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "File to import in the selected backend", required = true)
        public String input;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name to load the data", arity = 1)
        public String dbName;

    }

    @Parameters(commandNames = {"annotate-variants"}, commandDescription = "Create and load variant annotations into the database")
    public class AnnotateVariantsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;


        @Parameter(names = {"--create"}, description = "Run only the creation of the annotations to a file (specified by --output-filename)")
        public boolean create = false;

        @Parameter(names = {"--load"}, description = "Run only the load of the annotations into the DB from FILE")
        public String load = null;

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
        public VariantAnnotatorFactory.AnnotationSource annotator;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        public boolean overwriteAnnotations = false;

        @Deprecated
        @Parameter(names = {"--annotator-config"}, description = "Path to the file with the configuration of the annotator")
        public String annotatorConfig;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = true, arity = 1)
        public String dbName;

        @Deprecated
        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required =
                false, arity = 1)
        public String credentials;

        @Parameter(names = {"--output-filename"}, description = "Output file name. Default: dbName", required = false, arity = 1)
        public String fileName;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", required = false, arity = 1)
        public String outdir;

        @Parameter(names = {"--custom-name"}, description = "Provide a name to the custom annotation")
        public String customAnnotationKey = null;

        @Parameter(names = {"--species"}, description = "Species. Default hsapiens", required = false, arity = 1)
        public String species = "hsapiens";

        @Parameter(names = {"--assembly"}, description = "Assembly. Default GRc37", required = false, arity = 1)
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


    @Parameters(commandNames = {"benchmark"}, commandDescription = "Benchmark load and fetch variants with different databases")
    public class BenchmarkCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;


        @Parameter(names = {"--num-repetition"}, description = "Number of repetition", required = false, arity = 1)
        public int repetition = 3;

        @Parameter(names = {"--load"}, description = "File name with absolute path", required = false, arity = 1)
        public String load;

        @Parameter(names = {"--queries"}, description = "Queries to fetch the data from tables", required = false, arity = 1)
        public String queries;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = false, arity = 1)
        public String database;

        @Parameter(names = {"-t", "--table"}, description = "Benchmark variants", required = false, arity = 1)
        public String table;

        @Parameter(names = {"--host"}, description = "DataBase name", required = false, arity = 1)
        public String host;

        @Parameter(names = {"--concurrency"}, description = "Number of threads to run in parallel", required = false, arity = 1)
        public int concurrency = 1;

    }

    @Parameters(commandNames = {"stats-variants"}, commandDescription = "Create and load stats into a database.")
    public class StatsVariantsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;


        @Parameter(names = {"--create"}, description = "Run only the creation of the stats to a file")
        public boolean create = false;

        @Parameter(names = {"--load"}, description = "Load the stats from an already existing FILE directly into the database. FILE is a " +
                "prefix with structure <INPUT_FILENAME>.<TIME>")
        public boolean load = false;

        @Parameter(names = {"--overwrite-stats"}, description = "[PENDING] Overwrite stats in variants already present")
        public boolean overwriteStats = false;

        @Parameter(names = {"--update-stats"}, description = "Calculate stats just for missing positions. Assumes that existing stats are" +
                " correct")
        public boolean updateStats = false;

        @Parameter(names = {"-s", "--study-id"}, description = "Unique ID for the study where the file is classified", required = true,
                arity = 1)
        public int studyId;

        @Parameter(names = {"-f", "--file-id"}, description = "Calculate stats only for the selected file", required = false, arity = 1)
        public int fileId;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = false, arity = 1)
        public String dbName;

        @Parameter(names = {"--output-filename"}, description = "Output file name. Default: database name", required = false, arity = 1)
        public String fileName;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", required = false, arity = 1)
        public String outdir = ".";

        @DynamicParameter(names = {"--cohort-sample-ids"}, description = "Cohort definition with the schema -> <cohort-name>:<sample-id>" +
                "(,<sample-id>)* ", descriptionKey = "CohortName", assignment = ":")
        public Map<String, String> cohort = new HashMap<>();

        @DynamicParameter(names = {"--cohort-ids"}, description = "Cohort Ids for the cohorts to be inserted. If it is not provided, " +
                "cohortIds will be auto-generated.", assignment = ":")
        public Map<String, String> cohortIds = new HashMap<>();

        @Parameter(names = {"--study-configuration-file"}, description = "File with the study configuration. org.opencb.opencga.storage" +
                ".core.StudyConfiguration", required = false, arity = 1)
        public String studyConfigurationFile;

        @Parameter(names = {"--aggregated"}, description = "Select the type of aggregated VCF file: none, basic, EVS or ExAC", arity = 1)
        public VariantSource.Aggregation aggregated = VariantSource.Aggregation.NONE;

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF file")
        public String aggregationMappingFile;

    }
}
