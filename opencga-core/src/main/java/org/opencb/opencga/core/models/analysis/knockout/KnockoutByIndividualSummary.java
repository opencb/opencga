package org.opencb.opencga.core.models.analysis.knockout;

import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;

import java.util.Collections;
import java.util.List;

public class KnockoutByIndividualSummary {

    private String id;
    private String sampleId;
    private String motherId;
    private String motherSampleId;
    private String fatherId;
    private String fatherSampleId;
    private IndividualProperty.Sex sex;
    private List<Phenotype> phenotypes;
    private List<Disorder> disorders;

    private List<Gene> genes;
    private VariantStats variantStats;

    public KnockoutByIndividualSummary() {
    }

    public KnockoutByIndividualSummary(KnockoutByIndividual knockoutByIndividual) {
        this(knockoutByIndividual.getId(), knockoutByIndividual.getSampleId(), knockoutByIndividual.getMotherId(),
                knockoutByIndividual.getMotherSampleId(), knockoutByIndividual.getFatherId(), knockoutByIndividual.getFatherSampleId(),
                knockoutByIndividual.getSex(), knockoutByIndividual.getPhenotypes(), knockoutByIndividual.getDisorders(),
                Collections.emptyList(), null);
    }

    public KnockoutByIndividualSummary(String id, String sampleId, String motherId, String motherSampleId, String fatherId,
                                       String fatherSampleId, IndividualProperty.Sex sex, List<Phenotype> phenotypes,
                                       List<Disorder> disorders, List<Gene> genes, VariantStats variantStats) {
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

    public IndividualProperty.Sex getSex() {
        return sex;
    }

    public KnockoutByIndividualSummary setSex(IndividualProperty.Sex sex) {
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

    public List<Gene> getGenes() {
        return genes;
    }

    public KnockoutByIndividualSummary setGenes(List<Gene> genes) {
        this.genes = genes;
        return this;
    }

    public VariantStats getVariantStats() {
        return variantStats;
    }

    public KnockoutByIndividualSummary setVariantStats(VariantStats variantStats) {
        this.variantStats = variantStats;
        return this;
    }

    public static class Gene {
        private String id;
        private String name;

        public Gene() {
        }

        public Gene(String id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Gene{");
            sb.append("id='").append(id).append('\'');
            sb.append(", name='").append(name).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public Gene setId(String id) {
            this.id = id;
            return this;
        }

        public String getName() {
            return name;
        }

        public Gene setName(String name) {
            this.name = name;
            return this;
        }
    }

}
