package org.opencb.opencga.core.models.study.configuration;

public class StudyConfiguration {

    private ClinicalStudyConfiguration clinical;

    public StudyConfiguration() {
    }

    public StudyConfiguration(ClinicalStudyConfiguration clinical) {
        this.clinical = clinical;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyConfiguration{");
        sb.append("clinical=").append(clinical);
        sb.append('}');
        return sb.toString();
    }

    public ClinicalStudyConfiguration getClinical() {
        return clinical;
    }

    public StudyConfiguration setClinical(ClinicalStudyConfiguration clinical) {
        this.clinical = clinical;
        return this;
    }
}
