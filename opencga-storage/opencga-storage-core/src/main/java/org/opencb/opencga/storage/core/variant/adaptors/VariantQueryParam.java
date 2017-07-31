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

import static org.opencb.commons.datastore.core.QueryParam.Type.BOOLEAN;
import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT;
import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.ALL;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.NONE;

/**
 * Created on 30/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class VariantQueryParam implements QueryParam {

    private static final List<VariantQueryParam> VALUES = new ArrayList<>();

    public static final String ID_DESCR
            = "List of variant ids";
    public static final VariantQueryParam ID
            = new VariantQueryParam("ids", TEXT_ARRAY, ID_DESCR);

    public static final String REGION_DESCR
            = "List of regions: {chr}:{start}-{end}, e.g.: 2,3:1000000-2000000";
    public static final VariantQueryParam REGION
            = new VariantQueryParam("region", TEXT_ARRAY, REGION_DESCR);

    public static final String CHROMOSOME_DESCR
            = "List of chromosomes";
    public static final VariantQueryParam CHROMOSOME
            = new VariantQueryParam("chromosome", TEXT_ARRAY, CHROMOSOME_DESCR);

    public static final String GENE_DESCR
            = "List of genes";
    public static final VariantQueryParam GENE
            = new VariantQueryParam("gene", TEXT_ARRAY, GENE_DESCR);

    public static final String TYPE_DESCR
            = "Variant type: [SNV, MNV, INDEL, SV, CNV]";
    public static final VariantQueryParam TYPE
            = new VariantQueryParam("type", TEXT_ARRAY, TYPE_DESCR);

    public static final String REFERENCE_DESCR
            = "Reference allele";
    public static final VariantQueryParam REFERENCE
            = new VariantQueryParam("reference", TEXT_ARRAY, REFERENCE_DESCR);

    public static final String ALTERNATE_DESCR
            = "Main alternate allele";
    public static final VariantQueryParam ALTERNATE
            = new VariantQueryParam("alternate", TEXT_ARRAY, ALTERNATE_DESCR);


    public static final String STUDIES_DESCR
            = "";
    public static final VariantQueryParam STUDIES
            = new VariantQueryParam("studies", TEXT_ARRAY, STUDIES_DESCR);

    public static final String RETURNED_STUDIES_DESCR
            = "List of studies to be returned";
    public static final VariantQueryParam RETURNED_STUDIES
            = new VariantQueryParam("returnedStudies", TEXT_ARRAY, RETURNED_STUDIES_DESCR);

    //SAMPLES_DESCR = "Filter variants where ALL the provided samples are mutated (not HOM_REF or missing)";
    public static final String SAMPLES_DESCR
            = "Filter variants where ALL the provided samples are mutated (HET or HOM_ALT)";
    public static final VariantQueryParam SAMPLES
            = new VariantQueryParam("samples", TEXT_ARRAY, SAMPLES_DESCR);

    public static final String GENOTYPE_DESCR
            = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)*"
            + " e.g. HG0097:0/0;HG0098:0/1,1/1";
    public static final VariantQueryParam GENOTYPE
            = new VariantQueryParam("genotype", TEXT_ARRAY, GENOTYPE_DESCR);

    public static final String RETURNED_SAMPLES_DESCR
            = "List of samples to be returned. Accepts " + ALL + " and " + NONE;
    public static final VariantQueryParam RETURNED_SAMPLES
            = new VariantQueryParam("returnedSamples", TEXT_ARRAY, RETURNED_SAMPLES_DESCR);

    public static final String SAMPLES_METADATA_DESCR
            = "Returns the samples metadata group by study. Sample names will appear in the same order as their corresponding genotypes.";
    public static final VariantQueryParam SAMPLES_METADATA
            = new VariantQueryParam("samplesMetadata", TEXT_ARRAY, SAMPLES_METADATA_DESCR);

    public static final String INCLUDE_FORMAT_DESCR
            = "List of FORMAT names from Samples Data to include in the output. e.g: DP,AD. Accepts " + ALL + " and " + NONE;
    public static final VariantQueryParam INCLUDE_FORMAT = new VariantQueryParam("include-format", TEXT_ARRAY, INCLUDE_FORMAT_DESCR);

    public static final String INCLUDE_GENOTYPE_DESCR
            = "Include genotypes, apart of other formats defined with include-format";
    public static final VariantQueryParam INCLUDE_GENOTYPE = new VariantQueryParam("include-genotype", BOOLEAN, INCLUDE_GENOTYPE_DESCR);


    public static final String FILES_DESCR
            = "Select variants in specific files";
    public static final VariantQueryParam FILES
            = new VariantQueryParam("files", TEXT_ARRAY, FILES_DESCR);

    public static final String FILTER_DESCR
            = "Specify the FILTER for any of the files. If \"files\" filter is provided, will match the file and the filter."
            + " e.g.: PASS,LowGQX";
    public static final VariantQueryParam FILTER
            = new VariantQueryParam("filter", TEXT_ARRAY, FILTER_DESCR);

    public static final String RETURNED_FILES_DESCR
            = "List of files to be returned";
    public static final VariantQueryParam RETURNED_FILES
            = new VariantQueryParam("returnedFiles", TEXT_ARRAY, RETURNED_FILES_DESCR);


    public static final String COHORTS_DESCR
            = "Select variants with calculated stats for the selected cohorts";
    public static final VariantQueryParam COHORTS
            = new VariantQueryParam("cohorts", TEXT_ARRAY, COHORTS_DESCR);

    public static final String STATS_MAF_DESCR
            = "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}";
    public static final VariantQueryParam STATS_MAF
            = new VariantQueryParam("maf", TEXT_ARRAY, STATS_MAF_DESCR);

    public static final String STATS_MGF_DESCR
            = "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}";
    public static final VariantQueryParam STATS_MGF
            = new VariantQueryParam("mgf", TEXT_ARRAY, STATS_MGF_DESCR);

    public static final String MISSING_ALLELES_DESCR
            = "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}";
    public static final VariantQueryParam MISSING_ALLELES
            = new VariantQueryParam("missingAlleles", TEXT_ARRAY, MISSING_ALLELES_DESCR);

    public static final String MISSING_GENOTYPES_DESCR
            = "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}";
    public static final VariantQueryParam MISSING_GENOTYPES
            = new VariantQueryParam("missingGenotypes", TEXT_ARRAY, MISSING_GENOTYPES_DESCR);


    public static final String ANNOTATION_EXISTS_DESCR
            = "Specify if the variant annotation must exists.";
    public static final VariantQueryParam ANNOTATION_EXISTS
            = new VariantQueryParam("annotationExists", BOOLEAN, ANNOTATION_EXISTS_DESCR);

    public static final String ANNOT_CONSEQUENCE_TYPE_DESCR
            = "Consequence type SO term list. e.g. missense_variant,stop_lost or SO:0001583,SO:0001578";
    public static final VariantQueryParam ANNOT_CONSEQUENCE_TYPE
            = new VariantQueryParam("annot-ct", TEXT_ARRAY, ANNOT_CONSEQUENCE_TYPE_DESCR);

    public static final String ANNOT_XREF_DESCR
            = "External references.";
    public static final VariantQueryParam ANNOT_XREF
            = new VariantQueryParam("annot-xref", TEXT_ARRAY, ANNOT_XREF_DESCR);

    public static final String ANNOT_BIOTYPE_DESCR
            = "Biotype";
    public static final VariantQueryParam ANNOT_BIOTYPE
            = new VariantQueryParam("annot-biotype", TEXT_ARRAY, ANNOT_BIOTYPE_DESCR);

    public static final String ANNOT_POLYPHEN_DESCR
            = "Polyphen, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. <=0.9 , =benign";
    public static final VariantQueryParam ANNOT_POLYPHEN
            = new VariantQueryParam("polyphen", TEXT_ARRAY, ANNOT_POLYPHEN_DESCR);

    public static final String ANNOT_SIFT_DESCR
            = "Sift, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. >0.1 , ~=tolerant";
    public static final VariantQueryParam ANNOT_SIFT
            = new VariantQueryParam("sift", TEXT_ARRAY, ANNOT_SIFT_DESCR);

    public static final String ANNOT_PROTEIN_SUBSTITUTION_DESCR
            = "Protein substitution score. {protein_score}[<|>|<=|>=]{number} or"
            + " {protein_score}[~=|=]{description} e.g. polyphen>0.1 , sift=tolerant";
    public static final VariantQueryParam ANNOT_PROTEIN_SUBSTITUTION
            = new VariantQueryParam("protein_substitution", TEXT_ARRAY, ANNOT_PROTEIN_SUBSTITUTION_DESCR);

    public static final String ANNOT_CONSERVATION_DESCR
            = "Conservation score: {conservation_score}[<|>|<=|>=]{number}  e.g. phastCons>0.5,phylop<0.1,gerp>0.1";
    public static final VariantQueryParam ANNOT_CONSERVATION
            = new VariantQueryParam("conservation", TEXT_ARRAY, ANNOT_CONSERVATION_DESCR);

    public static final String ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR
            = "Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}";
    public static final VariantQueryParam ANNOT_POPULATION_ALTERNATE_FREQUENCY
            = new VariantQueryParam("alternate_frequency", TEXT_ARRAY, ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR);

    public static final String ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR
            = "Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}";
    public static final VariantQueryParam ANNOT_POPULATION_REFERENCE_FREQUENCY
            = new VariantQueryParam("reference_frequency", TEXT_ARRAY, ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR);

    public static final String ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR
            = "Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}";
    public static final VariantQueryParam ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY
            = new VariantQueryParam("annot-population-maf", TEXT_ARRAY, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR);

    public static final String ANNOT_TRANSCRIPTION_FLAGS_DESCR = "List of transcript annotation flags. e.g. CCDS,basic,cds_end_NF,"
            + "mRNA_end_NF,cds_start_NF,mRNA_start_NF,seleno";
    public static final VariantQueryParam ANNOT_TRANSCRIPTION_FLAGS
            = new VariantQueryParam("annot-transcription-flags", TEXT_ARRAY, ANNOT_TRANSCRIPTION_FLAGS_DESCR);

    public static final String ANNOT_GENE_TRAITS_ID_DESCR
            = "List of gene trait association id. e.g. \"umls:C0007222,OMIM:269600\"";
    public static final VariantQueryParam ANNOT_GENE_TRAITS_ID
            = new VariantQueryParam("annot-gene-trait-id", TEXT_ARRAY, ANNOT_GENE_TRAITS_ID_DESCR);

    public static final String ANNOT_GENE_TRAITS_NAME_DESCR
            = "List of gene trait association names. e.g. \"Cardiovascular Diseases\"";
    public static final VariantQueryParam ANNOT_GENE_TRAITS_NAME
            = new VariantQueryParam("annot-gene-trait-name", TEXT_ARRAY, ANNOT_GENE_TRAITS_NAME_DESCR);

    public static final String ANNOT_CLINVAR_DESCR
            = "List of ClinVar accessions";
    public static final VariantQueryParam ANNOT_CLINVAR
            = new VariantQueryParam("clinvar", TEXT_ARRAY, ANNOT_CLINVAR_DESCR);

    public static final String ANNOT_COSMIC_DESCR
            = "List of COSMIC mutation IDs.";
    public static final VariantQueryParam ANNOT_COSMIC
            = new VariantQueryParam("cosmic", TEXT_ARRAY, ANNOT_COSMIC_DESCR);

    public static final String ANNOT_TRAITS_DESCR
            = "List of traits, based on ClinVar, HPO, COSMIC, i.e.: IDs, histologies, descriptions,...";
    public static final VariantQueryParam ANNOT_TRAITS
            = new VariantQueryParam("traits", TEXT_ARRAY, ANNOT_TRAITS_DESCR);

    public static final String ANNOT_HPO_DESCR
            = "List of HPO terms. e.g. \"HP:0000545\"";
    public static final VariantQueryParam ANNOT_HPO
            = new VariantQueryParam("annot-hpo", TEXT_ARRAY, ANNOT_HPO_DESCR);

    public static final String ANNOT_GO_DESCR
            = "List of GO (Genome Ontology) terms. e.g. \"GO:0002020\"";
    public static final VariantQueryParam ANNOT_GO
            = new VariantQueryParam("annot-go", TEXT_ARRAY, ANNOT_GO_DESCR);

    public static final String ANNOT_EXPRESSION_DESCR
            = "List of tissues of interest. e.g. \"tongue\"";
    public static final VariantQueryParam ANNOT_EXPRESSION
            = new VariantQueryParam("annot-expression", TEXT_ARRAY, ANNOT_EXPRESSION_DESCR);

    public static final String ANNOT_PROTEIN_KEYWORDS_DESCR
            = "List of protein variant annotation keywords";
    public static final VariantQueryParam ANNOT_PROTEIN_KEYWORDS
            = new VariantQueryParam("annot-protein-keywords", TEXT_ARRAY, ANNOT_PROTEIN_KEYWORDS_DESCR);

    public static final String ANNOT_DRUG_DESCR
            = "List of drug names";
    public static final VariantQueryParam ANNOT_DRUG
            = new VariantQueryParam("annot-drug", TEXT_ARRAY, ANNOT_DRUG_DESCR);

    public static final String ANNOT_FUNCTIONAL_SCORE_DESCR
            = "Functional score: {functional_score}[<|>|<=|>=]{number}, e.g. cadd_scaled>5.2,cadd_raw<=0.3";
    public static final VariantQueryParam ANNOT_FUNCTIONAL_SCORE
            = new VariantQueryParam("annot-functional-score", TEXT_ARRAY, ANNOT_FUNCTIONAL_SCORE_DESCR);


    public static final String ANNOT_CUSTOM_DESCR
            = "Custom annotation: {key}[<|>|<=|>=]{number} or {key}[~=|=]{text}";
    public static final VariantQueryParam ANNOT_CUSTOM
            = new VariantQueryParam("annot-custom", TEXT_ARRAY, ANNOT_CUSTOM_DESCR);


    public static final String UNKNOWN_GENOTYPE_DESCR
            = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]";
    public static final VariantQueryParam UNKNOWN_GENOTYPE
            = new VariantQueryParam("unknownGenotype", TEXT, UNKNOWN_GENOTYPE_DESCR);

    private VariantQueryParam(String key, Type type, String description) {
        this.key = key;
        this.type = type;
        this.description = description;
        VALUES.add(this);
    }

    private final String key;
    private final Type type;
    private final String description;

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
