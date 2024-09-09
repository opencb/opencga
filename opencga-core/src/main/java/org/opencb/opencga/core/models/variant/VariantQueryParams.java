/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.core.models.variant;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.api.ParamConstants;

/**
 * When using native values (like boolean or int), set add
 * {@code @JsonInclude(JsonInclude.Include.NON_DEFAULT)} so they are null by default.
 */
public class VariantQueryParams extends BasicVariantQueryParams {

    public static final String SAMPLE_ANNOTATION_DESC =
            "Selects some samples using metadata information from Catalog. e.g. age>20;phenotype=hpo:123,hpo:456;name=smith";
    public static final String PROJECT_DESC = ParamConstants.PROJECT_DESCRIPTION;
    public static final String FAMILY_DESC = "Filter variants where any of the samples from the given family contains the variant "
            + "(HET or HOM_ALT)";
    public static final String FAMILY_MEMBERS_DESC = "Sub set of the members of a given family";
    public static final String FAMILY_DISORDER_DESC = "Specify the disorder to use for the family segregation";
    public static final String FAMILY_PROBAND_DESC = "Specify the proband child to use for the family segregation";
    public static final String FAMILY_SEGREGATION_DESCR = "Filter by segregation mode from a given family. Accepted values: "
            + "[ autosomalDominant, autosomalRecessive, XLinkedDominant, XLinkedRecessive, YLinked, mitochondrial, "
            + "deNovo, deNovoStrict, mendelianError, compoundHeterozygous ]";
    public static final String SAVED_FILTER_DESCR = "Use a saved filter at User level";
    public static final String PANEL_DESC = "Filter by genes from the given disease panel";
    public static final String PANEL_MOI_DESC = "Filter genes from specific panels that match certain mode of inheritance. " +
            "Accepted values : "
            + "[ autosomalDominant, autosomalRecessive, XLinkedDominant, XLinkedRecessive, YLinked, mitochondrial, "
            + "deNovo, mendelianError, compoundHeterozygous ]";
    public static final String PANEL_CONFIDENCE_DESC = "Filter genes from specific panels that match certain confidence. " +
            "Accepted values : [ high, medium, low, rejected ]";
    public static final String PANEL_INTERSECTION_DESC = "Intersect panel genes and regions with given "
            + "genes and regions from que input query. This will prevent returning variants from regions out of the panel.";
    public static final String PANEL_ROLE_IN_CANCER_DESC = "Filter genes from specific panels that match certain role in cancer. " +
            "Accepted values : [ both, oncogene, tumorSuppressorGene, fusion ]";
    public static final String PANEL_FEATURE_TYPE_DESC = "Filter elements from specific panels by type. " +
            "Accepted values : [ gene, region, str, variant ]";
    private static final String ACCEPTS_AND_OR = "Accepts AND ';' and OR ',' operators.";
    private static final String ACCEPTS_ALL_NONE = "Accepts '" + ParamConstants.ALL + "' and '" + ParamConstants.NONE + "'.";

    public static final String ID_DESCR
            = "List of variant IDs in the format chrom:start:ref:alt, e.g. 19:7177679:C:T";
    public static final String REGION_DESCR
            = "List of regions, these can be just a single chromosome name or regions in the format chr:start-end, e.g.: 2,3:100000-200000";
    @Deprecated
    public static final String CHROMOSOME_DESCR
            = "List of chromosomes, this is an alias of 'region' parameter with just the chromosome names";
    public static final String REFERENCE_DESCR
            = "Reference allele";
    public static final String ALTERNATE_DESCR
            = "Main alternate allele";
    public static final String TYPE_DESCR
            = "List of types, accepted values are SNV, MNV, INDEL, SV, COPY_NUMBER, COPY_NUMBER_LOSS, COPY_NUMBER_GAIN,"
        + " INSERTION, DELETION, DUPLICATION, TANDEM_DUPLICATION, BREAKEND, e.g. SNV,INDEL";
    public static final String STUDY_DESCR
            = "Filter variants from the given studies, these can be either the numeric ID or the alias with the format "
            + "organization@project:study";
    public static final String GENOTYPE_DESCR
            = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)*"
            + " e.g. HG0097:0/0;HG0098:0/1,1/1. "
            + "Unphased genotypes (e.g. 0/1, 1/1) will also include phased genotypes (e.g. 0|1, 1|0, 1|1), but not vice versa. "
            + "When filtering by multi-allelic genotypes, any secondary allele will match, regardless of its position"
            + " e.g. 1/2 will match with genotypes 1/2, 1/3, 1/4, .... "
            + "Genotype aliases accepted: HOM_REF, HOM_ALT, HET, HET_REF, HET_ALT, HET_MISS and MISS "
            + " e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT. "
            + "This will automatically set 'includeSample' parameter when not provided";
    public static final String SAMPLE_DATA_DESCR
            = "Filter by any SampleData field from samples. [{sample}:]{key}{op}{value}[,;]* . "
            + "If no sample is specified, will use all samples from \"sample\" or \"genotype\" filter. "
            + "e.g. DP>200 or HG0097:DP>200,HG0098:DP<10 . "
            + "Many FORMAT fields can be combined. e.g. HG0097:DP>200;GT=1/1,0/1,HG0098:DP<10";
    public static final String INCLUDE_SAMPLE_ID_DESCR
            = "Include sampleId on each result";
    public static final String SAMPLE_METADATA_DESCR
            = "Return the samples metadata group by study. Sample names will appear in the same order as their corresponding genotypes.";
    public static final String INCLUDE_GENOTYPE_DESCR
            = "Include genotypes, apart of other formats defined with includeFormat";
    public static final String SAMPLE_LIMIT_DESCR
            = "Limit the number of samples to be included in the result";
    public static final String SAMPLE_SKIP_DESCR
            = "Skip some samples from the result. Useful for sample pagination.";
    public static final String FILE_DESCR
            = "Filter variants from the files specified. This will set includeFile parameter when not provided";
    public static final String FILE_DATA_DESCR
            = "Filter by file data (i.e. FILTER, QUAL and INFO columns from VCF file). [{file}:]{key}{op}{value}[,;]* . "
            + "If no file is specified, will use all files from \"file\" filter. "
            + "e.g. AN>200 or file_1.vcf:AN>200;file_2.vcf:AN<10 . "
            + "Many fields can be combined. e.g. file_1.vcf:AN>200;DB=true;file_2.vcf:AN<10,FILTER=PASS,LowDP";
    public static final String FILTER_DESCR
            = "Specify the FILTER for any of the files. If 'file' filter is provided, will match the file and the filter. "
            + "e.g.: PASS,LowGQX";
    public static final String QUAL_DESCR
            = "Specify the QUAL for any of the files. If 'file' filter is provided, will match the file and the qual. "
            + "e.g.: >123.4";
    public static final String COHORT_DESCR
            = "Select variants with calculated stats for the selected cohorts";
    public static final String STATS_REF_DESCR
            = "Reference Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4";
    public static final String STATS_ALT_DESCR
            = "Alternate Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4";
    public static final String STATS_MAF_DESCR
            = "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4";
    public static final String STATS_MGF_DESCR
            = "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4";
    public static final String STATS_PASS_FREQ_DESCR
            = "Filter PASS frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL>0.8";
    public static final String MISSING_ALLELES_DESCR
            = "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}";
    public static final String MISSING_GENOTYPES_DESCR
            = "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}";
    public static final String SCORE_DESCR
            = "Filter by variant score: [{study:}]{score}[<|>|<=|>=]{number}";
    public static final String ANNOT_EXISTS_DESCR
            = "Return only annotated variants";
    public static final String ANNOT_XREF_DESCR
            = "List of any external reference, these can be genes, proteins or variants. "
            + "Accepted IDs include HGNC, Ensembl genes, dbSNP, ClinVar, HPO, Cosmic, HGVS ...";
    public static final String GENE_DESCR
            = "List of genes, most gene IDs are accepted (HGNC, Ensembl gene, ...). This is an alias to 'xref' parameter";
    public static final String ANNOT_BIOTYPE_DESCR
            = "List of biotypes, e.g. protein_coding";
    public static final String ANNOT_CONSEQUENCE_TYPE_DESCR
            = "List of SO consequence types, e.g. missense_variant,stop_lost or SO:0001583,SO:0001578. "
            + "Accepts aliases 'loss_of_function' and 'protein_altering'";
    @Deprecated
    public static final String ANNOT_POLYPHEN_DESCR
            = "Polyphen, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. <=0.9 , =benign";
    @Deprecated
    public static final String ANNOT_SIFT_DESCR
            = "Sift, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. >0.1 , ~=tolerant";
    public static final String ANNOT_PROTEIN_SUBSTITUTION_DESCR
            = "Protein substitution scores include SIFT and PolyPhen. You can query using the score {protein_score}[<|>|<=|>=]{number}"
            + " or the description {protein_score}[~=|=]{description} e.g. polyphen>0.1,sift=tolerant";
    public static final String ANNOT_CONSERVATION_DESCR
            = "Filter by conservation score: {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1";
    public static final String ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR
            = "Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. "
            + ParamConstants.POP_FREQ_1000G + ":ALL<0.01";
    public static final String ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR
            = "Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. "
            + ParamConstants.POP_FREQ_1000G + ":ALL<0.01";
    public static final String ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR
            = "Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. "
            + ParamConstants.POP_FREQ_1000G + ":ALL<0.01";
    public static final String ANNOT_TRANSCRIPT_FLAG_DESCR
            = "List of transcript flags. e.g. canonical, CCDS, basic, LRG, MANE Select, MANE Plus Clinical, EGLH_HaemOnc, TSO500";
    public static final String ANNOT_GENE_TRAIT_ID_DESCR
            = "List of gene trait association id. e.g. \"umls:C0007222\" , \"OMIM:269600\"";
    @Deprecated
    public static final String ANNOT_GENE_TRAIT_NAME_DESCR
            = "List of gene trait association names. e.g. Cardiovascular Diseases";
    public static final String ANNOT_TRAIT_DESCR
            = "List of traits, based on ClinVar, HPO, COSMIC, i.e.: IDs, histologies, descriptions,...";
    public static final String ANNOT_CLINICAL_DESCR
            = "Clinical source: clinvar, cosmic";
    public static final String ANNOT_CLINICAL_SIGNIFICANCE_DESCR
            = "Clinical significance: benign, likely_benign, likely_pathogenic, pathogenic";
    public static final String ANNOT_CLINICAL_CONFIRMED_STATUS_DESCR
            = "Clinical confirmed status";
    @Deprecated
    public static final String ANNOT_CLINVAR_DESCR
            = "List of ClinVar accessions";
    @Deprecated
    public static final String ANNOT_COSMIC_DESCR
            = "List of COSMIC mutation IDs.";
    @Deprecated
    public static final String ANNOT_HPO_DESCR
            = "List of HPO terms. e.g. \"HP:0000545,HP:0002812\"";
    public static final String ANNOT_GO_DESCR
            = "List of GO (Gene Ontology) terms. e.g. \"GO:0002020\"";
    public static final String ANNOT_EXPRESSION_DESCR
            = "List of tissues of interest. e.g. \"lung\"";
    public static final String ANNOT_GENE_ROLE_IN_CANCER_DESCR
            = "";
    public static final String ANNOT_PROTEIN_KEYWORD_DESCR
            = "List of Uniprot protein variant annotation keywords";
    public static final String ANNOT_DRUG_DESCR
            = "List of drug names";
    public static final String ANNOT_FUNCTIONAL_SCORE_DESCR
            = "Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3";
    public static final String CUSTOM_ANNOTATION_DESCR
            = "Custom annotation: {key}[<|>|<=|>=]{number} or {key}[~=|=]{text}";
    public static final String UNKNOWN_GENOTYPE_DESCR
            = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]";
    public static final String RELEASE_DESCR
            = "";
    public static final String SOURCE_DESCR = "Select the variant data source from where to fetch the data."
            + " Accepted values are 'variant_index' (default), 'secondary_annotation_index' and 'secondary_sample_index'. "
            + "When selecting a secondary_index, the data will be retrieved exclusively from that secondary index, "
            + "and the 'include/exclude' parameters will be ignored. "
            + "If the given query can not be fully resolved using the secondary index, an exception will be raised. "
            + "As the returned variants will only contain data from the secondary_index, some data might be missing or be partial.";
    public static final String INCLUDE_FILE_DESCR
            = "List of files to be returned. "
            + ACCEPTS_ALL_NONE + " If undefined, automatically includes files used for filtering. If none, no file is included.";
    public static final String INCLUDE_SAMPLE_DATA_DESCR
            = "List of Sample Data keys (i.e. FORMAT column from VCF file) from Sample Data to include in the output. e.g: DP,AD. "
            + ACCEPTS_ALL_NONE;
    public static final String INCLUDE_SAMPLE_DESCR
            = "List of samples to be included in the result. "
            + ACCEPTS_ALL_NONE + " If undefined, automatically includes samples used for filtering. If none, no sample is included.";
    public static final String INCLUDE_STUDY_DESCR
            = "List of studies to include in the result. "
            + ACCEPTS_ALL_NONE;
    public static final String SAMPLE_DESCR
            = "Filter variants by sample genotype. "
            + "This will automatically set 'includeSample' parameter when not provided. "
            + "This filter accepts multiple 3 forms: "
            + "1) List of samples: Samples that contain the main variant. " + ACCEPTS_AND_OR + " "
            + " e.g. HG0097,HG0098 . "
            + "2) List of samples with genotypes: {sample}:{gt1},{gt2}. " + ACCEPTS_AND_OR + " "
            + " e.g. HG0097:0/0;HG0098:0/1,1/1 . "
            + "Unphased genotypes (e.g. 0/1, 1/1) will also include phased genotypes (e.g. 0|1, 1|0, 1|1), but not vice versa. "
            + "When filtering by multi-allelic genotypes, any secondary allele will match, regardless of its position"
            + " e.g. 1/2 will match with genotypes 1/2, 1/3, 1/4, .... "
            + "Genotype aliases accepted: HOM_REF, HOM_ALT, HET, HET_REF, HET_ALT, HET_MISS and MISS "
            + " e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT . "
            + "3) Sample with segregation mode: {sample}:{segregation}. Only one sample accepted."
            + "Accepted segregation modes: "
            + "[ autosomalDominant, autosomalRecessive, XLinkedDominant, XLinkedRecessive, YLinked, mitochondrial, "
            + "deNovo, deNovoStrict, mendelianError, compoundHeterozygous ]. Value is case insensitive."
            + " e.g. HG0097:DeNovo "
            + "Sample must have parents defined and indexed. ";

    @DataField(description = SAVED_FILTER_DESCR)
    private String savedFilter;

    @DataField(description = CHROMOSOME_DESCR)
    private String chromosome;
    @DataField(description = REFERENCE_DESCR)
    private String reference;
    @DataField(description = ALTERNATE_DESCR)
    private String alternate;
    @DataField(description = RELEASE_DESCR)
    private String release;

    @DataField(description = INCLUDE_STUDY_DESCR)
    private String includeStudy;
    @DataField(description = INCLUDE_SAMPLE_DESCR)
    private String includeSample;
    @DataField(description = INCLUDE_FILE_DESCR)
    private String includeFile;
    @DataField(description = INCLUDE_SAMPLE_DATA_DESCR)
    private String includeSampleData;
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @DataField(description = INCLUDE_SAMPLE_ID_DESCR)
    private boolean includeSampleId;
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @DataField(description = INCLUDE_GENOTYPE_DESCR)
    private boolean includeGenotype;

    @DataField(description = FILE_DESCR)
    private String file;
    @DataField(description = QUAL_DESCR)
    private String qual;
    @DataField(description = FILTER_DESCR)
    private String filter;
    @DataField(description = FILE_DATA_DESCR)
    private String fileData;

    @DataField(description = GENOTYPE_DESCR)
    private String genotype;
    @DataField(description = SAMPLE_DESCR)
    private String sample;
    @DataField(description = SAMPLE_LIMIT_DESCR)
    private Integer sampleLimit;
    @DataField(description = SAMPLE_SKIP_DESCR)
    private Integer sampleSkip;
    @DataField(description = SAMPLE_DATA_DESCR)
    private String sampleData;
    @DataField(description = SAMPLE_ANNOTATION_DESC)
    private String sampleAnnotation;

    @DataField(description = FAMILY_DESC)
    private String family;
    @DataField(description = FAMILY_MEMBERS_DESC)
    private String familyMembers;
    @DataField(description = FAMILY_DISORDER_DESC)
    private String familyDisorder;
    @DataField(description = FAMILY_PROBAND_DESC)
    private String familyProband;
    @DataField(description = FAMILY_SEGREGATION_DESCR)
    private String familySegregation;

    @DataField(description = COHORT_DESCR)
    private String cohort;
    @DataField(description = STATS_PASS_FREQ_DESCR)
    private String cohortStatsPass;
    @DataField(description = STATS_MGF_DESCR)
    private String cohortStatsMgf;
    @DataField(description = MISSING_ALLELES_DESCR)
    private String missingAlleles;
    @DataField(description = MISSING_GENOTYPES_DESCR)
    private String missingGenotypes;
    @DataField(description = ANNOT_EXISTS_DESCR)
    private Boolean annotationExists;

    @DataField(description = SCORE_DESCR)
    private String score;

    @DataField(description = ANNOT_POLYPHEN_DESCR)
    @Deprecated private String polyphen;
    @DataField(description = ANNOT_SIFT_DESCR)
    @Deprecated private String sift;
    @DataField(description = ANNOT_GENE_ROLE_IN_CANCER_DESCR)
    private String geneRoleInCancer;
    @DataField(description = ANNOT_GENE_TRAIT_ID_DESCR)
    private String geneTraitId;
    @DataField(description = ANNOT_GENE_TRAIT_NAME_DESCR)
    private String geneTraitName;
    @DataField(description = ANNOT_TRAIT_DESCR)
    private String trait;
    @DataField(description = ANNOT_COSMIC_DESCR)
    private String cosmic;
    @DataField(description = ANNOT_CLINICAL_DESCR)
    private String clinvar;
    @DataField(description = ANNOT_HPO_DESCR)
    private String hpo;
    @DataField(description = ANNOT_GO_DESCR)
    private String go;
    @DataField(description = ANNOT_EXPRESSION_DESCR)
    private String expression;
    @DataField(description = ANNOT_PROTEIN_KEYWORD_DESCR)
    private String proteinKeyword;
    @DataField(description = ANNOT_DRUG_DESCR)
    private String drug;
    @DataField(description = CUSTOM_ANNOTATION_DESCR)
    private String customAnnotation;
    @DataField(since = "3.2.1", description = SOURCE_DESCR)
    private String source;

    @DataField(description = UNKNOWN_GENOTYPE_DESCR)
    private String unknownGenotype;
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @DataField(description = SAMPLE_METADATA_DESCR)
    private boolean sampleMetadata = false;
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @DataField(description = "Sort the results by chromosome, start, end and alternate allele")
    private boolean sort = false;


    public VariantQueryParams() {
    }

    public VariantQueryParams(Query query) {
        appendQuery(query);
    }

    public VariantQueryParams appendQuery(Query query) {
        updateParams(query);
        return this;
    }

    public String getSavedFilter() {
        return savedFilter;
    }

    public VariantQueryParams setSavedFilter(String savedFilter) {
        this.savedFilter = savedFilter;
        return this;
    }

    public String getChromosome() {
        return chromosome;
    }

    public VariantQueryParams setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public String getReference() {
        return reference;
    }

    public VariantQueryParams setReference(String reference) {
        this.reference = reference;
        return this;
    }

    public String getAlternate() {
        return alternate;
    }

    public VariantQueryParams setAlternate(String alternate) {
        this.alternate = alternate;
        return this;
    }

    public String getRelease() {
        return release;
    }

    public VariantQueryParams setRelease(String release) {
        this.release = release;
        return this;
    }

    public String getIncludeStudy() {
        return includeStudy;
    }

    public VariantQueryParams setIncludeStudy(String includeStudy) {
        this.includeStudy = includeStudy;
        return this;
    }

    public String getIncludeSample() {
        return includeSample;
    }

    public VariantQueryParams setIncludeSample(String includeSample) {
        this.includeSample = includeSample;
        return this;
    }

    public String getIncludeFile() {
        return includeFile;
    }

    public VariantQueryParams setIncludeFile(String includeFile) {
        this.includeFile = includeFile;
        return this;
    }

    public String getIncludeSampleData() {
        return includeSampleData;
    }

    public VariantQueryParams setIncludeSampleData(String includeSampleData) {
        this.includeSampleData = includeSampleData;
        return this;
    }

    public boolean getIncludeGenotype() {
        return includeGenotype;
    }

    public VariantQueryParams setIncludeGenotype(boolean includeGenotype) {
        this.includeGenotype = includeGenotype;
        return this;
    }

    public boolean getIncludeSampleId() {
        return includeSampleId;
    }

    public VariantQueryParams setIncludeSampleId(boolean includeSampleId) {
        this.includeSampleId = includeSampleId;
        return this;
    }

    public String getFile() {
        return file;
    }

    public VariantQueryParams setFile(String file) {
        this.file = file;
        return this;
    }

    public String getQual() {
        return qual;
    }

    public VariantQueryParams setQual(String qual) {
        this.qual = qual;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public VariantQueryParams setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getFileData() {
        return fileData;
    }

    public VariantQueryParams setFileData(String fileData) {
        this.fileData = fileData;
        return this;
    }

    public String getGenotype() {
        return genotype;
    }

    public VariantQueryParams setGenotype(String genotype) {
        this.genotype = genotype;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public VariantQueryParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public Integer getSampleLimit() {
        return sampleLimit;
    }

    public VariantQueryParams setSampleLimit(Integer sampleLimit) {
        this.sampleLimit = sampleLimit;
        return this;
    }

    public Integer getSampleSkip() {
        return sampleSkip;
    }

    public VariantQueryParams setSampleSkip(Integer sampleSkip) {
        this.sampleSkip = sampleSkip;
        return this;
    }

    public String getSampleData() {
        return sampleData;
    }

    public VariantQueryParams setSampleData(String sampleData) {
        this.sampleData = sampleData;
        return this;
    }

    public String getSampleAnnotation() {
        return sampleAnnotation;
    }

    public VariantQueryParams setSampleAnnotation(String sampleAnnotation) {
        this.sampleAnnotation = sampleAnnotation;
        return this;
    }

    public String getFamily() {
        return family;
    }

    public VariantQueryParams setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getFamilyMembers() {
        return familyMembers;
    }

    public VariantQueryParams setFamilyMembers(String familyMembers) {
        this.familyMembers = familyMembers;
        return this;
    }

    public String getFamilyDisorder() {
        return familyDisorder;
    }

    public VariantQueryParams setFamilyDisorder(String familyDisorder) {
        this.familyDisorder = familyDisorder;
        return this;
    }

    public String getFamilyProband() {
        return familyProband;
    }

    public VariantQueryParams setFamilyProband(String familyProband) {
        this.familyProband = familyProband;
        return this;
    }

    public String getFamilySegregation() {
        return familySegregation;
    }

    public VariantQueryParams setFamilySegregation(String familySegregation) {
        this.familySegregation = familySegregation;
        return this;
    }

    public String getCohort() {
        return cohort;
    }

    public VariantQueryParams setCohort(String cohort) {
        this.cohort = cohort;
        return this;
    }

    public String getCohortStatsMgf() {
        return cohortStatsMgf;
    }

    public VariantQueryParams setCohortStatsMgf(String cohortStatsMgf) {
        this.cohortStatsMgf = cohortStatsMgf;
        return this;
    }

    public String getCohortStatsPass() {
        return cohortStatsPass;
    }

    public VariantQueryParams setCohortStatsPass(String cohortStatsPass) {
        this.cohortStatsPass = cohortStatsPass;
        return this;
    }

    public String getMissingAlleles() {
        return missingAlleles;
    }

    public VariantQueryParams setMissingAlleles(String missingAlleles) {
        this.missingAlleles = missingAlleles;
        return this;
    }

    public String getMissingGenotypes() {
        return missingGenotypes;
    }

    public VariantQueryParams setMissingGenotypes(String missingGenotypes) {
        this.missingGenotypes = missingGenotypes;
        return this;
    }

    public Boolean getAnnotationExists() {
        return annotationExists;
    }

    public VariantQueryParams setAnnotationExists(Boolean annotationExists) {
        this.annotationExists = annotationExists;
        return this;
    }

    public String getScore() {
        return score;
    }

    public VariantQueryParams setScore(String score) {
        this.score = score;
        return this;
    }

    public String getPolyphen() {
        return polyphen;
    }

    public VariantQueryParams setPolyphen(String polyphen) {
        this.polyphen = polyphen;
        return this;
    }

    public String getSift() {
        return sift;
    }

    public VariantQueryParams setSift(String sift) {
        this.sift = sift;
        return this;
    }

    public String getGeneRoleInCancer() {
        return geneRoleInCancer;
    }

    public VariantQueryParams setGeneRoleInCancer(String geneRoleInCancer) {
        this.geneRoleInCancer = geneRoleInCancer;
        return this;
    }

    public String getGeneTraitId() {
        return geneTraitId;
    }

    public VariantQueryParams setGeneTraitId(String geneTraitId) {
        this.geneTraitId = geneTraitId;
        return this;
    }

    public String getGeneTraitName() {
        return geneTraitName;
    }

    public VariantQueryParams setGeneTraitName(String geneTraitName) {
        this.geneTraitName = geneTraitName;
        return this;
    }

    public String getTrait() {
        return trait;
    }

    public VariantQueryParams setTrait(String trait) {
        this.trait = trait;
        return this;
    }

    public String getCosmic() {
        return cosmic;
    }

    public VariantQueryParams setCosmic(String cosmic) {
        this.cosmic = cosmic;
        return this;
    }

    public String getClinvar() {
        return clinvar;
    }

    public VariantQueryParams setClinvar(String clinvar) {
        this.clinvar = clinvar;
        return this;
    }

    public String getHpo() {
        return hpo;
    }

    public VariantQueryParams setHpo(String hpo) {
        this.hpo = hpo;
        return this;
    }

    public String getGo() {
        return go;
    }

    public VariantQueryParams setGo(String go) {
        this.go = go;
        return this;
    }

    public String getExpression() {
        return expression;
    }

    public VariantQueryParams setExpression(String expression) {
        this.expression = expression;
        return this;
    }

    public String getProteinKeyword() {
        return proteinKeyword;
    }

    public VariantQueryParams setProteinKeyword(String proteinKeyword) {
        this.proteinKeyword = proteinKeyword;
        return this;
    }

    public String getDrug() {
        return drug;
    }

    public VariantQueryParams setDrug(String drug) {
        this.drug = drug;
        return this;
    }

    public String getCustomAnnotation() {
        return customAnnotation;
    }

    public VariantQueryParams setCustomAnnotation(String customAnnotation) {
        this.customAnnotation = customAnnotation;
        return this;
    }

    public String getUnknownGenotype() {
        return unknownGenotype;
    }

    public VariantQueryParams setUnknownGenotype(String unknownGenotype) {
        this.unknownGenotype = unknownGenotype;
        return this;
    }

    public String getSource() {
        return source;
    }

    public VariantQueryParams setSource(String source) {
        this.source = source;
        return this;
    }

    public boolean isSampleMetadata() {
        return sampleMetadata;
    }

    public VariantQueryParams setSampleMetadata(boolean sampleMetadata) {
        this.sampleMetadata = sampleMetadata;
        return this;
    }

    public boolean isSort() {
        return sort;
    }

    public VariantQueryParams setSort(boolean sort) {
        this.sort = sort;
        return this;
    }

}
