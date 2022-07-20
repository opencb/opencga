package org.opencb.opencga.core.models.study;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class StudyVariantEngineConfiguration {

    @DataField(description = ParamConstants.STUDY_VARIANT_ENGINE_CONFIGURATION_OPTIONS_DESCRIPTION)
    private ObjectMap options;
    @DataField(description = ParamConstants.STUDY_VARIANT_ENGINE_CONFIGURATION_SAMPLE_INDEX_DESCRIPTION)
    private SampleIndexConfiguration sampleIndex;

    public StudyVariantEngineConfiguration() {
    }

    public StudyVariantEngineConfiguration(ObjectMap options, SampleIndexConfiguration sampleIndex) {
        this.options = options;
        this.sampleIndex = sampleIndex;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public StudyVariantEngineConfiguration setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }

    public SampleIndexConfiguration getSampleIndex() {
        return sampleIndex;
    }

    public StudyVariantEngineConfiguration setSampleIndex(SampleIndexConfiguration sampleIndex) {
        this.sampleIndex = sampleIndex;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyVariantEngineConfiguration{");
        sb.append("options=").append(options.toJson());
        sb.append(", sampleIndex=").append(sampleIndex);
        sb.append('}');
        return sb.toString();
    }
}
