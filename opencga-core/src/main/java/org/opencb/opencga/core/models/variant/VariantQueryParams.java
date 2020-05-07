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

import org.opencb.commons.datastore.core.Query;

/**
 * Do not use native values (like boolean or int), so they are null by default.
 */
public class VariantQueryParams extends BasicVariantQueryParams {

    private String savedFilter;

    private String chromosome;
    private String reference;
    private String alternate;
    private String release;

    private String includeStudy;
    private String includeSample;
    private String includeFile;
    private String includeSampleData;
    private String includeSampleId;
    private String includeGenotype;

    private String file;
    private String qual;
    private String filter;
    private String fileData;

    private String genotype;
    private String sample;
    private Integer sampleLimit;
    private Integer sampleSkip;
    private String sampleData;
    private String sampleAnnotation;

    private String family;
    private String familyMembers;
    private String familyDisorder;
    private String familyProband;
    private String familySegregation;

    private String cohort;
    private String cohortStatsPass;
    private String cohortStatsMgf;
    private String missingAlleles;
    private String missingGenotypes;
    private Boolean annotationExists;

    private String score;

    @Deprecated private String polyphen;
    @Deprecated private String sift;
    private String geneTraitId;
    private String geneTraitName;
    private String trait;
    private String cosmic;
    private String clinvar;
    private String hpo;
    private String go;
    private String expression;
    private String proteinKeyword;
    private String drug;
    private String customAnnotation;

    private String unknownGenotype;
    private boolean sampleMetadata = false;
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

    public String getIncludeGenotype() {
        return includeGenotype;
    }

    public VariantQueryParams setIncludeGenotype(String includeGenotype) {
        this.includeGenotype = includeGenotype;
        return this;
    }

    public String getIncludeSampleId() {
        return includeSampleId;
    }

    public VariantQueryParams setIncludeSampleId(String includeSampleId) {
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
