package org.opencb.opencga.core.models.analysis.knockout;

import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private List<String> genes;
    private int numVariants;
    private int numHomAlt;
    private int numCompHet;
    private int numHetAlt;
    private int numDelOverlap;

    public KnockoutByIndividualSummary() {
    }

    public KnockoutByIndividualSummary(KnockoutByIndividual knockoutByIndividual) {
        this(knockoutByIndividual.getId(), knockoutByIndividual.getSampleId(), knockoutByIndividual.getMotherId(),
                knockoutByIndividual.getMotherSampleId(), knockoutByIndividual.getFatherId(), knockoutByIndividual.getFatherSampleId(),
                knockoutByIndividual.getSex(), knockoutByIndividual.getPhenotypes(), knockoutByIndividual.getDisorders(),
                Collections.emptyList(), 0, 0, 0, 0, 0);
    }

    public KnockoutByIndividualSummary(String id, String sampleId, String motherId, String motherSampleId, String fatherId,
                                       String fatherSampleId, IndividualProperty.Sex sex, List<Phenotype> phenotypes,
                                       List<Disorder> disorders, List<String> genes, int numVariants, int numHomAlt, int numCompHet,
                                       int numHetAlt, int numDelOverlap) {
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
        this.numVariants = numVariants;
        this.numHomAlt = numHomAlt;
        this.numCompHet = numCompHet;
        this.numHetAlt = numHetAlt;
        this.numDelOverlap = numDelOverlap;
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
        sb.append(", numVariants=").append(numVariants);
        sb.append(", numHomAlt=").append(numHomAlt);
        sb.append(", numCompHet=").append(numCompHet);
        sb.append(", numHetAlt=").append(numHetAlt);
        sb.append(", numDelOverlap=").append(numDelOverlap);
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

    public List<String> getGenes() {
        return genes;
    }

    public KnockoutByIndividualSummary setGenes(List<String> genes) {
        this.genes = genes;
        return this;
    }

    public int getNumVariants() {
        return numVariants;
    }

    public KnockoutByIndividualSummary setNumVariants(int numVariants) {
        this.numVariants = numVariants;
        return this;
    }

    public int getNumHomAlt() {
        return numHomAlt;
    }

    public KnockoutByIndividualSummary setNumHomAlt(int numHomAlt) {
        this.numHomAlt = numHomAlt;
        return this;
    }

    public int getNumCompHet() {
        return numCompHet;
    }

    public KnockoutByIndividualSummary setNumCompHet(int numCompHet) {
        this.numCompHet = numCompHet;
        return this;
    }

    public int getNumHetAlt() {
        return numHetAlt;
    }

    public KnockoutByIndividualSummary setNumHetAlt(int numHetAlt) {
        this.numHetAlt = numHetAlt;
        return this;
    }

    public int getNumDelOverlap() {
        return numDelOverlap;
    }

    public KnockoutByIndividualSummary setNumDelOverlap(int numDelOverlap) {
        this.numDelOverlap = numDelOverlap;
        return this;
    }
}
