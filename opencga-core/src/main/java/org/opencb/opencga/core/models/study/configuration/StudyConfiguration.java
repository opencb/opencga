package org.opencb.opencga.core.models.study.configuration;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.core.models.study.StudyVariantEngineConfiguration;

public class StudyConfiguration {

    private ClinicalAnalysisStudyConfiguration clinical;
    private StudyVariantEngineConfiguration variantEngine;

    public StudyConfiguration() {
    }

    public StudyConfiguration(ClinicalAnalysisStudyConfiguration clinical, StudyVariantEngineConfiguration variantEngine) {
        this.clinical = clinical;
        this.variantEngine = variantEngine;
    }

    public static StudyConfiguration init() {
        return new StudyConfiguration(ClinicalAnalysisStudyConfiguration.defaultConfiguration(),
                new StudyVariantEngineConfiguration(new ObjectMap(), SampleIndexConfiguration.defaultConfiguration()));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyConfiguration{");
        sb.append("clinical=").append(clinical);
        sb.append(", variantEngine=").append(variantEngine);
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

    public StudyVariantEngineConfiguration getVariantEngine() {
        return variantEngine;
    }

    public StudyConfiguration setVariantEngine(StudyVariantEngineConfiguration variantEngine) {
        this.variantEngine = variantEngine;
        return this;
    }
}
