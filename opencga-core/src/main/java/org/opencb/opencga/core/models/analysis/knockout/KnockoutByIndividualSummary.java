package org.opencb.opencga.core.models.analysis.knockout;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;

import java.util.Collections;
import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class KnockoutByIndividualSummary {

    @DataField(description = ParamConstants.KNOCKOUT_BY_INDIVIDUAL_SUMMARY_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.KNOCKOUT_BY_INDIVIDUAL_SUMMARY_SAMPLE_ID_DESCRIPTION)
    private String sampleId;
    @DataField(description = ParamConstants.KNOCKOUT_BY_INDIVIDUAL_SUMMARY_MOTHER_ID_DESCRIPTION)
    private String motherId;
    @DataField(description = ParamConstants.KNOCKOUT_BY_INDIVIDUAL_SUMMARY_MOTHER_SAMPLE_ID_DESCRIPTION)
    private String motherSampleId;
    @DataField(description = ParamConstants.KNOCKOUT_BY_INDIVIDUAL_SUMMARY_FATHER_ID_DESCRIPTION)
    private String fatherId;
    @DataField(description = ParamConstants.KNOCKOUT_BY_INDIVIDUAL_SUMMARY_FATHER_SAMPLE_ID_DESCRIPTION)
    private String fatherSampleId;
    @DataField(description = ParamConstants.KNOCKOUT_BY_INDIVIDUAL_SUMMARY_SEX_DESCRIPTION)
    private String sex;
    @DataField(description = ParamConstants.KNOCKOUT_BY_INDIVIDUAL_SUMMARY_PHENOTYPES_DESCRIPTION)
    private List<Phenotype> phenotypes;
    @DataField(description = ParamConstants.KNOCKOUT_BY_INDIVIDUAL_SUMMARY_DISORDERS_DESCRIPTION)
    private List<Disorder> disorders;

    @DataField(description = ParamConstants.KNOCKOUT_BY_INDIVIDUAL_SUMMARY_NUM_PARENTS_DESCRIPTION)
    private int numParents;
    @DataField(description = ParamConstants.KNOCKOUT_BY_INDIVIDUAL_SUMMARY_GENES_DESCRIPTION)
    private List<String> genes;
    @DataField(description = ParamConstants.KNOCKOUT_BY_INDIVIDUAL_SUMMARY_VARIANT_STATS_DESCRIPTION)
    private VariantKnockoutStats variantStats;

    public KnockoutByIndividualSummary() {
    }

    public KnockoutByIndividualSummary(KnockoutByIndividual knockoutByIndividual) {
        this(knockoutByIndividual.getId(), knockoutByIndividual.getSampleId(), knockoutByIndividual.getMotherId(),
                knockoutByIndividual.getMotherSampleId(), knockoutByIndividual.getFatherId(), knockoutByIndividual.getFatherSampleId(),
                knockoutByIndividual.getSex(), knockoutByIndividual.getPhenotypes(), knockoutByIndividual.getDisorders(),
                Collections.emptyList(), null);
    }

    public KnockoutByIndividualSummary(String id, String sampleId, String motherId, String motherSampleId, String fatherId,
                                       String fatherSampleId, String sex, List<Phenotype> phenotypes,
                                       List<Disorder> disorders, List<String> genes, VariantKnockoutStats variantStats) {
        this.id = id;
        this.sampleId = sampleId;
        this.motherId = motherId;
        this.motherSampleId = motherSampleId;
        this.fatherId = fatherId;
        this.fatherSampleId = fatherSampleId;
        this.sex = sex;
        this.phenotypes = phenotypes;
        this.disorders = disorders;
        this.genes = genes;
        this.variantStats = variantStats;
        this.numParents = 0;
        if (StringUtils.isNotEmpty(motherSampleId)) {
            this.numParents++;
        }
        if (StringUtils.isNotEmpty(fatherSampleId)) {
            this.numParents++;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KnockoutByIndividualSummary{");
        sb.append("id='").append(id).append('\'');
        sb.append(", sampleId='").append(sampleId).append('\'');
        sb.append(", motherId='").append(motherId).append('\'');
        sb.append(", motherSampleId='").append(motherSampleId).append('\'');
        sb.append(", fatherId='").append(fatherId).append('\'');
        sb.append(", fatherSampleId='").append(fatherSampleId).append('\'');
        sb.append(", sex=").append(sex);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", disorders=").append(disorders);
        sb.append(", genes=").append(genes);
        sb.append(", variantStats=").append(variantStats);
        sb.append(", numParents=").append(numParents);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public KnockoutByIndividualSummary setId(String id) {
        this.id = id;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public KnockoutByIndividualSummary setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getMotherId() {
        return motherId;
    }

    public KnockoutByIndividualSummary setMotherId(String motherId) {
        this.motherId = motherId;
        return this;
    }

    public String getMotherSampleId() {
        return motherSampleId;
    }

    public KnockoutByIndividualSummary setMotherSampleId(String motherSampleId) {
        this.motherSampleId = motherSampleId;
        return this;
    }

    public String getFatherId() {
        return fatherId;
    }

    public KnockoutByIndividualSummary setFatherId(String fatherId) {
        this.fatherId = fatherId;
        return this;
    }

    public String getFatherSampleId() {
        return fatherSampleId;
    }

    public KnockoutByIndividualSummary setFatherSampleId(String fatherSampleId) {
        this.fatherSampleId = fatherSampleId;
        return this;
    }

    public String getSex() {
        return sex;
    }

    public KnockoutByIndividualSummary setSex(String sex) {
        this.sex = sex;
        return this;
    }

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public KnockoutByIndividualSummary setPhenotypes(List<Phenotype> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<Disorder> getDisorders() {
        return disorders;
    }

    public KnockoutByIndividualSummary setDisorders(List<Disorder> disorders) {
        this.disorders = disorders;
        return this;
    }

    public List<String> getGenes() {
        return genes;
    }

    public KnockoutByIndividualSummary setGenes(List<String> genes) {
        this.genes = genes;
        return this;
    }

    public VariantKnockoutStats getVariantStats() {
        return variantStats;
    }

    public KnockoutByIndividualSummary setVariantStats(VariantKnockoutStats variantStats) {
        this.variantStats = variantStats;
        return this;
    }

    public int getNumParents() {
        return numParents;
    }

    public KnockoutByIndividualSummary setNumParents(int numParents) {
        this.numParents = numParents;
        return this;
    }
}
