package org.opencb.opencga.app.cli.analysis.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.app.cli.GeneralCliOptions;

/**
 * Created by pfurio on 23/11/16.
 */
@Parameters(commandNames = {"variant"}, commandDescription = "Variant commands")
public class VariantCommandOptions {

    public JCommander jCommander;
    public GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    public IndexCommandOptions indexCommandOptions;
    public QueryVariantCommandOptions queryCommandOptions;

    public VariantCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;
        this.indexCommandOptions = new IndexCommandOptions();
        this.queryCommandOptions = new QueryVariantCommandOptions();
    }

    @Parameters(commandNames = {"index"}, commandDescription = "Index VCF files")
    public class IndexCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--file-id"}, description = "Comma separated list of file ids (files or directories)", required = true, arity = 1)
        public String fileIds;

        @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = false, arity = 1)
        public String studyId;

        @Parameter(names = {"--transform"}, description = "If present it only runs the transform stage, no load is executed")
        public boolean transform;

        @Parameter(names = {"--load"}, description = "If present only the load stage is executed, transformation is skipped")
        public boolean load;

        @Parameter(names = {"--outdir"}, description = "Directory where transformed index files will be stored", required = false, arity = 1)
        public String outdirId;

        /**
         * @deprecated This field should be detected automatically.
         */
        @Deprecated
        @Parameter(names = {"--exclude-genotypes"}, description = "Index excluding the genotype information")
        public boolean excludeGenotype;

        @Parameter(names = {"--include-extra-fields"}, description = "Index including other genotype fields [CSV]")
        public String extraFields;

        @Parameter(names = {"--aggregated"}, description = "Select the type of aggregated VCF file: none, basic, EVS or ExAC", arity = 1)
        public VariantSource.Aggregation aggregated = VariantSource.Aggregation.NONE;

//        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF " +
//                "file")
//        public String aggregationMappingFile = null;
//
//        @Parameter(names = {"--gvcf"}, description = "The input file is in gvcf format")
//        public boolean gvcf;
//
//        @Parameter(names = {"--bgzip"}, description = "[PENDING] The input file is in bgzip format")
//        public boolean bgzip;

        @Parameter(names = {"--calculate-stats"}, description = "Calculate indexed variants statistics after the load step")
        public boolean calculateStats = false;

        @Parameter(names = {"--annotate"}, description = "Annotate indexed variants after the load step")
        public boolean annotate = false;
//
//        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
//        public org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.AnnotationSource annotator = null;

//        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations already present in variants")
//        public boolean overwriteAnnotations;

        @Parameter(names = {"--resume"}, description = "Resume a previously failed indexation", arity = 0)
        public boolean resume;
    }

    @Parameters(commandNames = {"query"}, commandDescription = "Search over indexed variants")
    public class QueryVariantCommandOptions {

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

        @Parameter(names = {"--samples-metadata"}, description = "Returns the samples metadata group by studyId, instead of the variants",
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

    }

}
