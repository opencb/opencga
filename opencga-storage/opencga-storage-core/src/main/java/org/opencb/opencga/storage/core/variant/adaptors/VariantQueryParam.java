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

import java.util.*;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;
import static org.opencb.opencga.core.models.variant.VariantQueryParams.*;

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
    private static final Map<String, VariantQueryParam> VALUES_MAP = new HashMap<>();

    public static final VariantQueryParam ID = new VariantQueryParam("id", TEXT_ARRAY, ID_DESCR);
    public static final VariantQueryParam REGION = new VariantQueryParam("region", TEXT_ARRAY, REGION_DESCR);
    public static final VariantQueryParam REFERENCE = new VariantQueryParam("reference", TEXT_ARRAY, REFERENCE_DESCR);
    public static final VariantQueryParam ALTERNATE = new VariantQueryParam("alternate", TEXT_ARRAY, ALTERNATE_DESCR);
    public static final VariantQueryParam TYPE = new VariantQueryParam("type", TEXT_ARRAY, TYPE_DESCR);
    public static final VariantQueryParam STUDY = new VariantQueryParam("study", TEXT_ARRAY, STUDY_DESCR);
    public static final VariantQueryParam INCLUDE_STUDY = new VariantQueryParam("includeStudy", TEXT_ARRAY, INCLUDE_STUDY_DESCR);
    public static final VariantQueryParam SAMPLE = new VariantQueryParam("sample", TEXT_ARRAY, SAMPLE_DESCR);
    public static final VariantQueryParam GENOTYPE = new VariantQueryParam("genotype", TEXT_ARRAY, GENOTYPE_DESCR);
    public static final VariantQueryParam SAMPLE_DATA = new VariantQueryParam("sampleData", TEXT_ARRAY, SAMPLE_DATA_DESCR);
    public static final VariantQueryParam INCLUDE_SAMPLE = new VariantQueryParam("includeSample", TEXT_ARRAY, INCLUDE_SAMPLE_DESCR);
    public static final VariantQueryParam INCLUDE_SAMPLE_ID = new VariantQueryParam("includeSampleId", BOOLEAN, INCLUDE_SAMPLE_ID_DESCR);
    public static final VariantQueryParam SAMPLE_METADATA = new VariantQueryParam("sampleMetadata", BOOLEAN, SAMPLE_METADATA_DESCR);
    public static final VariantQueryParam INCLUDE_SAMPLE_DATA = new VariantQueryParam("includeSampleData",
            TEXT_ARRAY, INCLUDE_SAMPLE_DATA_DESCR);
    public static final VariantQueryParam INCLUDE_GENOTYPE = new VariantQueryParam("includeGenotype", BOOLEAN, INCLUDE_GENOTYPE_DESCR);
    public static final VariantQueryParam SAMPLE_LIMIT = new VariantQueryParam("sampleLimit", INTEGER, SAMPLE_LIMIT_DESCR);
    public static final VariantQueryParam SAMPLE_SKIP = new VariantQueryParam("sampleSkip", INTEGER, SAMPLE_SKIP_DESCR);
    public static final VariantQueryParam FILE = new VariantQueryParam("file", TEXT_ARRAY, FILE_DESCR);
    public static final VariantQueryParam FILE_DATA = new VariantQueryParam("fileData", TEXT_ARRAY, FILE_DATA_DESCR);
    public static final VariantQueryParam FILTER = new VariantQueryParam("filter", TEXT_ARRAY, FILTER_DESCR);
    public static final VariantQueryParam QUAL = new VariantQueryParam("qual", DECIMAL_ARRAY, QUAL_DESCR);
    public static final VariantQueryParam INCLUDE_FILE = new VariantQueryParam("includeFile", TEXT_ARRAY, INCLUDE_FILE_DESCR);
    public static final VariantQueryParam COHORT = new VariantQueryParam("cohort", TEXT_ARRAY, COHORT_DESCR);
    public static final VariantQueryParam STATS_REF = new VariantQueryParam("cohortStatsRef", TEXT_ARRAY, STATS_REF_DESCR);
    public static final VariantQueryParam STATS_ALT = new VariantQueryParam("cohortStatsAlt", TEXT_ARRAY, STATS_ALT_DESCR);
    public static final VariantQueryParam STATS_MAF = new VariantQueryParam("cohortStatsMaf", TEXT_ARRAY, STATS_MAF_DESCR);
    public static final VariantQueryParam STATS_MGF = new VariantQueryParam("cohortStatsMgf", TEXT_ARRAY, STATS_MGF_DESCR);
    public static final VariantQueryParam STATS_PASS_FREQ = new VariantQueryParam("cohortStatsPass", TEXT_ARRAY, STATS_PASS_FREQ_DESCR);
    public static final VariantQueryParam MISSING_ALLELES = new VariantQueryParam("missingAlleles", TEXT_ARRAY, MISSING_ALLELES_DESCR);
    public static final VariantQueryParam MISSING_GENOTYPES
            = new VariantQueryParam("missingGenotypes", TEXT_ARRAY, MISSING_GENOTYPES_DESCR);
    public static final VariantQueryParam SCORE
            = new VariantQueryParam("score", TEXT_ARRAY, MISSING_GENOTYPES_DESCR);
    public static final VariantQueryParam ANNOTATION_EXISTS = new VariantQueryParam("annotationExists", BOOLEAN, ANNOT_EXISTS_DESCR);
    public static final VariantQueryParam ANNOT_XREF = new VariantQueryParam("xref", TEXT_ARRAY, ANNOT_XREF_DESCR);
    public static final VariantQueryParam GENE = new VariantQueryParam("gene", TEXT_ARRAY, GENE_DESCR);
    public static final VariantQueryParam ANNOT_BIOTYPE = new VariantQueryParam("biotype", TEXT_ARRAY, ANNOT_BIOTYPE_DESCR);
    public static final VariantQueryParam ANNOT_CONSEQUENCE_TYPE = new VariantQueryParam("ct", TEXT_ARRAY, ANNOT_CONSEQUENCE_TYPE_DESCR);
    public static final VariantQueryParam ANNOT_PROTEIN_SUBSTITUTION
            = new VariantQueryParam("proteinSubstitution", TEXT_ARRAY, ANNOT_PROTEIN_SUBSTITUTION_DESCR);
    public static final VariantQueryParam ANNOT_CONSERVATION
            = new VariantQueryParam("conservation", TEXT_ARRAY, ANNOT_CONSERVATION_DESCR);
    public static final VariantQueryParam ANNOT_POPULATION_ALTERNATE_FREQUENCY
            = new VariantQueryParam("populationFrequencyAlt", TEXT_ARRAY, ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR);
    public static final VariantQueryParam ANNOT_POPULATION_REFERENCE_FREQUENCY
            = new VariantQueryParam("populationFrequencyRef", TEXT_ARRAY, ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR);
    public static final VariantQueryParam ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY
            = new VariantQueryParam("populationFrequencyMaf", TEXT_ARRAY, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR);
    public static final VariantQueryParam ANNOT_TRANSCRIPT_FLAG
            = new VariantQueryParam("transcriptFlag", TEXT_ARRAY, ANNOT_TRANSCRIPT_FLAG_DESCR);
    public static final VariantQueryParam ANNOT_GENE_TRAIT_ID
            = new VariantQueryParam("geneTraitId", TEXT_ARRAY, ANNOT_GENE_TRAIT_ID_DESCR);
    public static final VariantQueryParam ANNOT_TRAIT = new VariantQueryParam("trait", TEXT_ARRAY, ANNOT_TRAIT_DESCR);
    public static final VariantQueryParam ANNOT_CLINICAL =
            new VariantQueryParam("clinical", TEXT_ARRAY, ANNOT_CLINICAL_DESCR);
    public static final VariantQueryParam ANNOT_CLINICAL_SIGNIFICANCE =
            new VariantQueryParam("clinicalSignificance", TEXT_ARRAY, ANNOT_CLINICAL_SIGNIFICANCE_DESCR);
    public static final VariantQueryParam ANNOT_CLINICAL_CONFIRMED_STATUS =
            new VariantQueryParam("clinicalConfirmedStatus", BOOLEAN, ANNOT_CLINICAL_CONFIRMED_STATUS_DESCR);
    public static final VariantQueryParam ANNOT_GO = new VariantQueryParam("go", TEXT_ARRAY, ANNOT_GO_DESCR);
    public static final VariantQueryParam ANNOT_EXPRESSION = new VariantQueryParam("expression", TEXT_ARRAY, ANNOT_EXPRESSION_DESCR);
    public static final VariantQueryParam ANNOT_GENE_ROLE_IN_CANCER
            = new VariantQueryParam("geneRoleInCancer", TEXT_ARRAY, ANNOT_GENE_ROLE_IN_CANCER_DESCR);
    public static final VariantQueryParam ANNOT_PROTEIN_KEYWORD
            = new VariantQueryParam("proteinKeyword", TEXT_ARRAY, ANNOT_PROTEIN_KEYWORD_DESCR);
    public static final VariantQueryParam ANNOT_DRUG = new VariantQueryParam("drug", TEXT_ARRAY, ANNOT_DRUG_DESCR);
    public static final VariantQueryParam ANNOT_FUNCTIONAL_SCORE
            = new VariantQueryParam("functionalScore", TEXT_ARRAY, ANNOT_FUNCTIONAL_SCORE_DESCR);
    public static final VariantQueryParam CUSTOM_ANNOTATION
            = new VariantQueryParam("customAnnotation", TEXT_ARRAY, CUSTOM_ANNOTATION_DESCR);
    public static final VariantQueryParam UNKNOWN_GENOTYPE = new VariantQueryParam("unknownGenotype", TEXT, UNKNOWN_GENOTYPE_DESCR);
    public static final VariantQueryParam RELEASE = new VariantQueryParam("release", INTEGER, RELEASE_DESCR);
    public static final VariantQueryParam SOURCE = new VariantQueryParam("source", TEXT, SOURCE_DESCR);

    @Deprecated
    public static final VariantQueryParam ANNOT_GENE_TRAIT_NAME
            = new VariantQueryParam("geneTraitName", TEXT_ARRAY, ANNOT_GENE_TRAIT_NAME_DESCR);
    @Deprecated
    public static final VariantQueryParam ANNOT_POLYPHEN
            = new VariantQueryParam("polyphen", TEXT_ARRAY, ANNOT_POLYPHEN_DESCR);
    @Deprecated
    public static final VariantQueryParam ANNOT_SIFT
            = new VariantQueryParam("sift", TEXT_ARRAY, ANNOT_SIFT_DESCR);
    @Deprecated
    public static final VariantQueryParam ANNOT_CLINVAR = new VariantQueryParam("clinvar", TEXT_ARRAY, ANNOT_CLINVAR_DESCR);
    @Deprecated
    public static final VariantQueryParam ANNOT_COSMIC = new VariantQueryParam("cosmic", TEXT_ARRAY, ANNOT_COSMIC_DESCR);
    @Deprecated
    public static final VariantQueryParam ANNOT_HPO = new VariantQueryParam("hpo", TEXT_ARRAY, ANNOT_HPO_DESCR);

    private VariantQueryParam(String key, Type type, String description) {
        this.key = key;
        this.type = type;
        this.description = description;

        VALUES.add(this);
        if (VALUES_MAP.put(key, this) != null) {
            throw new IllegalStateException("Found two VariantQueryParams with same key '" + key + "'.");
        }
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

    public static VariantQueryParam valueOf(String param) {
        return VALUES_MAP.get(param);
    }
}
