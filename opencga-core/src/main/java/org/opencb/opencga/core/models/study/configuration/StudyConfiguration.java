package org.opencb.opencga.core.models.study.configuration;

public class StudyConfiguration {

    private ClinicalAnalysisStudyConfiguration clinical;

    public StudyConfiguration() {
    }

    public StudyConfiguration(ClinicalAnalysisStudyConfiguration clinical) {
        this.clinical = clinical;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyConfiguration{");
        sb.append("clinical=").append(clinical);
        sb.append('}');
        return sb.toString();
    }

    public ClinicalAnalysisStudyConfiguration getClinical() {
        return clinical;
    }

    public StudyConfiguration setClinical(ClinicalAnalysisStudyConfiguration clinical) {
        this.clinical = clinical;
        return this;
    }
}
