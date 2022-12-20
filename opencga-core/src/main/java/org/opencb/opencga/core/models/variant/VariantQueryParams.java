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
import org.opencb.commons.datastore.core.Query;

/**
 * When using native values (like boolean or int), set add
 * {@code @JsonInclude(JsonInclude.Include.NON_DEFAULT)} so they are null by default.
 */
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantQueryParams extends BasicVariantQueryParams {

    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_SAVED_FILTER_DESCRIPTION)
    private String savedFilter;

    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_CHROMOSOME_DESCRIPTION)
    private String chromosome;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_REFERENCE_DESCRIPTION)
    private String reference;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_ALTERNATE_DESCRIPTION)
    private String alternate;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_RELEASE_DESCRIPTION)
    private String release;

    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_INCLUDE_STUDY_DESCRIPTION)
    private String includeStudy;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_INCLUDE_SAMPLE_DESCRIPTION)
    private String includeSample;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_INCLUDE_FILE_DESCRIPTION)
    private String includeFile;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_INCLUDE_SAMPLE_DATA_DESCRIPTION)
    private String includeSampleData;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_INCLUDE_SAMPLE_ID_DESCRIPTION)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private boolean includeSampleId;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_INCLUDE_GENOTYPE_DESCRIPTION)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private boolean includeGenotype;

    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_FILE_DESCRIPTION)
    private String file;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_QUAL_DESCRIPTION)
    private String qual;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_FILTER_DESCRIPTION)
    private String filter;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_FILE_DATA_DESCRIPTION)
    private String fileData;

    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_GENOTYPE_DESCRIPTION)
    private String genotype;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_SAMPLE_DESCRIPTION)
    private String sample;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_SAMPLE_LIMIT_DESCRIPTION)
    private Integer sampleLimit;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_SAMPLE_SKIP_DESCRIPTION)
    private Integer sampleSkip;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_SAMPLE_DATA_DESCRIPTION)
    private String sampleData;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_SAMPLE_ANNOTATION_DESCRIPTION)
    private String sampleAnnotation;

    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_FAMILY_DESCRIPTION)
    private String family;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_FAMILY_MEMBERS_DESCRIPTION)
    private String familyMembers;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_FAMILY_DISORDER_DESCRIPTION)
    private String familyDisorder;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_FAMILY_PROBAND_DESCRIPTION)
    private String familyProband;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_FAMILY_SEGREGATION_DESCRIPTION)
    private String familySegregation;

    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_COHORT_DESCRIPTION)
    private String cohort;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_COHORT_STATS_PASS_DESCRIPTION)
    private String cohortStatsPass;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_COHORT_STATS_MGF_DESCRIPTION)
    private String cohortStatsMgf;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_MISSING_ALLELES_DESCRIPTION)
    private String missingAlleles;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_MISSING_GENOTYPES_DESCRIPTION)
    private String missingGenotypes;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_ANNOTATION_EXISTS_DESCRIPTION)
    private Boolean annotationExists;

    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_SCORE_DESCRIPTION)
    private String score;

    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_POLYPHEN_DESCRIPTION)
    @Deprecated private String polyphen;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_SIFT_DESCRIPTION)
    @Deprecated private String sift;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_GENE_TRAIT_ID_DESCRIPTION)
    private String geneTraitId;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_GENE_TRAIT_NAME_DESCRIPTION)
    private String geneTraitName;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_TRAIT_DESCRIPTION)
    private String trait;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_COSMIC_DESCRIPTION)
    private String cosmic;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_CLINVAR_DESCRIPTION)
    private String clinvar;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_HPO_DESCRIPTION)
    private String hpo;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_GO_DESCRIPTION)
    private String go;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_EXPRESSION_DESCRIPTION)
    private String expression;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_PROTEIN_KEYWORD_DESCRIPTION)
    private String proteinKeyword;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_DRUG_DESCRIPTION)
    private String drug;
    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_CUSTOM_ANNOTATION_DESCRIPTION)
    private String customAnnotation;

    @DataField(description = ParamConstants.VARIANT_QUERY_PARAMS_UNKNOWN_GENOTYPE_DESCRIPTION)
    private String unknownGenotype;
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private boolean sampleMetadata = false;
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
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
