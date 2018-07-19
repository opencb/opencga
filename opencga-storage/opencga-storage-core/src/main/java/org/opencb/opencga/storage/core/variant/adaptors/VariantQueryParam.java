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

package org.opencb.opencga.storage.core.variant.adaptors;

import org.opencb.commons.datastore.core.QueryParam;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.ALL;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.NONE;

/**
 * Created on 30/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class VariantQueryParam implements QueryParam {

    private final String key;
    private final Type type;
    private final String description;

    private static final List<VariantQueryParam> VALUES = new ArrayList<>();


    public static final String ID_DESCR
            = "List of IDs, these can be rs IDs (dbSNP) or variants in the format chrom:start:ref:alt, e.g. rs116600158,19:7177679:C:T";
    public static final VariantQueryParam ID = new VariantQueryParam("id", TEXT_ARRAY, ID_DESCR);

    public static final String REGION_DESCR
            = "List of regions, these can be just a single chromosome name or regions in the format chr:start-end, e.g.: 2,3:100000-200000";
    public static final VariantQueryParam REGION = new VariantQueryParam("region", TEXT_ARRAY, REGION_DESCR);

    @Deprecated
    public static final String CHROMOSOME_DESCR
            = "List of chromosomes, this is an alias of 'region' parameter with just the chromosome names";

    public static final String REFERENCE_DESCR
            = "Reference allele";
    public static final VariantQueryParam REFERENCE = new VariantQueryParam("reference", TEXT_ARRAY, REFERENCE_DESCR);

    public static final String ALTERNATE_DESCR
            = "Main alternate allele";
    public static final VariantQueryParam ALTERNATE = new VariantQueryParam("alternate", TEXT_ARRAY, ALTERNATE_DESCR);

    public static final String TYPE_DESCR
            = "List of types, accepted values are SNV, MNV, INDEL, SV, CNV, INSERTION, DELETION, e.g. SNV,INDEL";
    public static final VariantQueryParam TYPE = new VariantQueryParam("type", TEXT_ARRAY, TYPE_DESCR);


    public static final String STUDY_DESCR
            = "Filter variants from the given studies, these can be either the numeric ID or the alias with the format user@project:study";
    public static final VariantQueryParam STUDY = new VariantQueryParam("study", TEXT_ARRAY, STUDY_DESCR);

    public static final String INCLUDE_STUDY_DESCR
            = "List of studies to include in the result";
    public static final VariantQueryParam INCLUDE_STUDY = new VariantQueryParam("includeStudy", TEXT_ARRAY, INCLUDE_STUDY_DESCR);

    //SAMPLES_DESCR = "Filter variants where ALL the provided samples are mutated (not HOM_REF or missing)";
    public static final String SAMPLE_DESCR
            = "Filter variants where the samples contain the variant (HET or HOM_ALT). "
            + "Accepts AND (" + VariantQueryUtils.AND + ") and OR (" + VariantQueryUtils.OR + ") operators. "
            + "This will automatically set 'includeSample' parameter when not provided";
    public static final VariantQueryParam SAMPLE = new VariantQueryParam("sample", TEXT_ARRAY, SAMPLE_DESCR);

    public static final String GENOTYPE_DESCR
            = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)*"
            + " e.g. HG0097:0/0;HG0098:0/1,1/1. "
            + "Genotype aliases accepted: HOM_REF, HOM_ALT, HET, HET_REF, HET_ALT and MISS "
            + " e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT. "
            + "This will automatically set 'includeSample' parameter when not provided";
    public static final VariantQueryParam GENOTYPE = new VariantQueryParam("genotype", TEXT_ARRAY, GENOTYPE_DESCR);

    public static final String INCLUDE_SAMPLE_DESCR
            = "List of samples to be included in the result, this accepts " + ALL + " and " + NONE;
    public static final VariantQueryParam INCLUDE_SAMPLE = new VariantQueryParam("includeSample", TEXT_ARRAY, INCLUDE_SAMPLE_DESCR);

    public static final String SAMPLE_METADATA_DESCR
            = "Return the samples metadata group by study. Sample names will appear in the same order as their corresponding genotypes.";
    public static final VariantQueryParam SAMPLE_METADATA = new VariantQueryParam("sampleMetadata", TEXT_ARRAY, SAMPLE_METADATA_DESCR);

    public static final String INCLUDE_FORMAT_DESCR
            = "List of FORMAT names from Samples Data to include in the output. e.g: DP,AD. Accepts " + ALL + " and " + NONE;
    public static final VariantQueryParam INCLUDE_FORMAT = new VariantQueryParam("includeFormat", TEXT_ARRAY, INCLUDE_FORMAT_DESCR);

    public static final String INCLUDE_GENOTYPE_DESCR
            = "Include genotypes, apart of other formats defined with include-format";
    public static final VariantQueryParam INCLUDE_GENOTYPE = new VariantQueryParam("includeGenotype", BOOLEAN, INCLUDE_GENOTYPE_DESCR);

    public static final String FILE_DESCR
            = "Filter variants from the files specified. This will set includeFile parameter when not provided";
    public static final VariantQueryParam FILE = new VariantQueryParam("file", TEXT_ARRAY, FILE_DESCR);

    public static final String FILTER_DESCR
            = "Specify the FILTER for any of the files. If 'file' filter is provided, will match the file and the filter. "
            + "e.g.: PASS,LowGQX";
    public static final VariantQueryParam FILTER = new VariantQueryParam("filter", TEXT_ARRAY, FILTER_DESCR);

    public static final String QUAL_DESCR
            = "Specify the QUAL for any of the files. If 'file' filter is provided, will match the file and the qual. "
            + "e.g.: >123.4";
    public static final VariantQueryParam QUAL = new VariantQueryParam("qual", DECIMAL_ARRAY, QUAL_DESCR);

    public static final String INCLUDE_FILE_DESCR
            = "List of files to be returned";
    public static final VariantQueryParam INCLUDE_FILE = new VariantQueryParam("includeFile", TEXT_ARRAY, INCLUDE_FILE_DESCR);

    public static final String COHORT_DESCR
            = "Select variants with calculated stats for the selected cohorts";
    public static final VariantQueryParam COHORT = new VariantQueryParam("cohort", TEXT_ARRAY, COHORT_DESCR);

    public static final String STATS_MAF_DESCR
            = "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}";
    public static final VariantQueryParam STATS_MAF = new VariantQueryParam("maf", TEXT_ARRAY, STATS_MAF_DESCR);

    public static final String STATS_MGF_DESCR
            = "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}";
    public static final VariantQueryParam STATS_MGF = new VariantQueryParam("mgf", TEXT_ARRAY, STATS_MGF_DESCR);

    public static final String MISSING_ALLELES_DESCR
            = "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}";
    public static final VariantQueryParam MISSING_ALLELES = new VariantQueryParam("missingAlleles", TEXT_ARRAY, MISSING_ALLELES_DESCR);

    public static final String MISSING_GENOTYPES_DESCR
            = "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}";
    public static final VariantQueryParam MISSING_GENOTYPES
            = new VariantQueryParam("missingGenotypes", TEXT_ARRAY, MISSING_GENOTYPES_DESCR);


    public static final String ANNOT_EXISTS_DESCR
            = "Return only annotated variants";
    public static final VariantQueryParam ANNOTATION_EXISTS = new VariantQueryParam("annotationExists", BOOLEAN, ANNOT_EXISTS_DESCR);

    public static final String ANNOT_XREF_DESCR
            = "List of any external reference, these can be genes, proteins or variants. "
            + "Accepted IDs include HGNC, Ensembl genes, dbSNP, ClinVar, HPO, Cosmic, ...";
    public static final VariantQueryParam ANNOT_XREF = new VariantQueryParam("xref", TEXT_ARRAY, ANNOT_XREF_DESCR);

    public static final String GENE_DESCR
            = "List of genes, most gene IDs are accepted (HGNC, Ensembl gene, ...). This is an alias to 'xref' parameter";
    public static final VariantQueryParam GENE = new VariantQueryParam("gene", TEXT_ARRAY, GENE_DESCR);

    public static final String ANNOT_BIOTYPE_DESCR
            = "List of biotypes, e.g. protein_coding";
    public static final VariantQueryParam ANNOT_BIOTYPE = new VariantQueryParam("biotype", TEXT_ARRAY, ANNOT_BIOTYPE_DESCR);

    public static final String ANNOT_CONSEQUENCE_TYPE_DESCR
            = "List of SO consequence types, e.g. missense_variant,stop_lost or SO:0001583,SO:0001578";
    public static final VariantQueryParam ANNOT_CONSEQUENCE_TYPE = new VariantQueryParam("ct", TEXT_ARRAY, ANNOT_CONSEQUENCE_TYPE_DESCR);

    @Deprecated
    public static final String ANNOT_POLYPHEN_DESCR
            = "Polyphen, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. <=0.9 , =benign";
    @Deprecated
    public static final VariantQueryParam ANNOT_POLYPHEN
            = new VariantQueryParam("polyphen", TEXT_ARRAY, ANNOT_POLYPHEN_DESCR);

    @Deprecated
    public static final String ANNOT_SIFT_DESCR
            = "Sift, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. >0.1 , ~=tolerant";
    @Deprecated
    public static final VariantQueryParam ANNOT_SIFT
            = new VariantQueryParam("sift", TEXT_ARRAY, ANNOT_SIFT_DESCR);

    public static final String ANNOT_PROTEIN_SUBSTITUTION_DESCR
            = "Protein substitution scores include SIFT and PolyPhen. You can query using the score {protein_score}[<|>|<=|>=]{number}"
            + " or the description {protein_score}[~=|=]{description} e.g. polyphen>0.1,sift=tolerant";
    public static final VariantQueryParam ANNOT_PROTEIN_SUBSTITUTION
            = new VariantQueryParam("proteinSubstitution", TEXT_ARRAY, ANNOT_PROTEIN_SUBSTITUTION_DESCR);

    public static final String ANNOT_CONSERVATION_DESCR
            = "Filter by conservation score: {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1";
    public static final VariantQueryParam ANNOT_CONSERVATION
            = new VariantQueryParam("conservation", TEXT_ARRAY, ANNOT_CONSERVATION_DESCR);

    public static final String ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR
            = "Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}";
    public static final VariantQueryParam ANNOT_POPULATION_ALTERNATE_FREQUENCY
            = new VariantQueryParam("populationFrequencyAlt", TEXT_ARRAY, ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR);

    public static final String ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR
            = "Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}";
    public static final VariantQueryParam ANNOT_POPULATION_REFERENCE_FREQUENCY
            = new VariantQueryParam("populationFrequencyRef", TEXT_ARRAY, ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR);

    public static final String ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR
            = "Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}";
    public static final VariantQueryParam ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY
            = new VariantQueryParam("populationFrequencyMaf", TEXT_ARRAY, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR);

    public static final String ANNOT_TRANSCRIPTION_FLAG_DESCR
            = "List of transcript annotation flags. e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno";
    public static final VariantQueryParam ANNOT_TRANSCRIPTION_FLAG
            = new VariantQueryParam("transcriptionFlag", TEXT_ARRAY, ANNOT_TRANSCRIPTION_FLAG_DESCR);

    public static final String ANNOT_GENE_TRAIT_ID_DESCR
            = "List of gene trait association id. e.g. \"umls:C0007222\" , \"OMIM:269600\"";
    public static final VariantQueryParam ANNOT_GENE_TRAIT_ID
            = new VariantQueryParam("geneTraitId", TEXT_ARRAY, ANNOT_GENE_TRAIT_ID_DESCR);

    @Deprecated
    public static final String ANNOT_GENE_TRAIT_NAME_DESCR
            = "List of gene trait association names. e.g. Cardiovascular Diseases";
    @Deprecated
    public static final VariantQueryParam ANNOT_GENE_TRAIT_NAME
            = new VariantQueryParam("geneTraitName", TEXT_ARRAY, ANNOT_GENE_TRAIT_NAME_DESCR);

    public static final String ANNOT_TRAIT_DESCR
            = "List of traits, based on ClinVar, HPO, COSMIC, i.e.: IDs, histologies, descriptions,...";
    public static final VariantQueryParam ANNOT_TRAIT = new VariantQueryParam("trait", TEXT_ARRAY, ANNOT_TRAIT_DESCR);

    public static final String ANNOT_CLINICAL_SIGNIFICANCE_DESCR
            = "";
    public static final VariantQueryParam ANNOT_CLINICAL_SIGNIFICANCE =
            new VariantQueryParam("clinicalSignificance", TEXT_ARRAY, ANNOT_CLINICAL_SIGNIFICANCE_DESCR);

    @Deprecated
    public static final String ANNOT_CLINVAR_DESCR
            = "List of ClinVar accessions";
    @Deprecated
    public static final VariantQueryParam ANNOT_CLINVAR = new VariantQueryParam("clinvar", TEXT_ARRAY, ANNOT_CLINVAR_DESCR);

    @Deprecated
    public static final String ANNOT_COSMIC_DESCR
            = "List of COSMIC mutation IDs.";
    @Deprecated
    public static final VariantQueryParam ANNOT_COSMIC = new VariantQueryParam("cosmic", TEXT_ARRAY, ANNOT_COSMIC_DESCR);

    @Deprecated
    public static final String ANNOT_HPO_DESCR
            = "List of HPO terms. e.g. HP:0000545";
    @Deprecated
    public static final VariantQueryParam ANNOT_HPO = new VariantQueryParam("hpo", TEXT_ARRAY, ANNOT_HPO_DESCR);

    public static final String ANNOT_GO_DESCR
            = "List of GO (Gene Ontology) terms. e.g. GO:0002020";
    public static final VariantQueryParam ANNOT_GO = new VariantQueryParam("go", TEXT_ARRAY, ANNOT_GO_DESCR);

    public static final String ANNOT_EXPRESSION_DESCR
            = "List of tissues of interest. e.g. \"lung\"";
    public static final VariantQueryParam ANNOT_EXPRESSION = new VariantQueryParam("expression", TEXT_ARRAY, ANNOT_EXPRESSION_DESCR);

    public static final String ANNOT_PROTEIN_KEYWORD_DESCR
            = "List of protein variant annotation keywords";
    public static final VariantQueryParam ANNOT_PROTEIN_KEYWORD
            = new VariantQueryParam("proteinKeyword", TEXT_ARRAY, ANNOT_PROTEIN_KEYWORD_DESCR);

    public static final String ANNOT_DRUG_DESCR
            = "List of drug names";
    public static final VariantQueryParam ANNOT_DRUG = new VariantQueryParam("drug", TEXT_ARRAY, ANNOT_DRUG_DESCR);

    public static final String ANNOT_FUNCTIONAL_SCORE_DESCR
            = "Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3";
    public static final VariantQueryParam ANNOT_FUNCTIONAL_SCORE
            = new VariantQueryParam("functionalScore", TEXT_ARRAY, ANNOT_FUNCTIONAL_SCORE_DESCR);

    public static final String CUSTOM_ANNOTATION_DESCR
            = "Custom annotation: {key}[<|>|<=|>=]{number} or {key}[~=|=]{text}";
    public static final VariantQueryParam CUSTOM_ANNOTATION
            = new VariantQueryParam("customAnnotation", TEXT_ARRAY, CUSTOM_ANNOTATION_DESCR);

    public static final String UNKNOWN_GENOTYPE_DESCR
            = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]";
    public static final VariantQueryParam UNKNOWN_GENOTYPE = new VariantQueryParam("unknownGenotype", TEXT, UNKNOWN_GENOTYPE_DESCR);


    public static final String RELEASE_DESCR
            = "";
    public static final VariantQueryParam RELEASE
            = new VariantQueryParam("release", INTEGER, RELEASE_DESCR);

    private VariantQueryParam(String key, Type type, String description) {
        this.key = key;
        this.type = type;
        this.description = description;

        VALUES.add(this);
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public String toString() {
        return key() + " [" + type() + "] : " + description();
    }

    public static List<VariantQueryParam> values() {
        return Collections.unmodifiableList(VALUES);
    }
}
