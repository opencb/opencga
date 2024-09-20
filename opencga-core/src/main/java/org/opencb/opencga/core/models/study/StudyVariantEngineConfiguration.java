package org.opencb.opencga.core.models.study;

import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;

public class StudyVariantEngineConfiguration {

    private ObjectMap options;
    private SampleIndexConfiguration sampleIndex;

    @DataField(description = "Variant setup run", since = "3.2.0")
    private VariantSetupResult setup;

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

    public VariantSetupResult getSetup() {
        return setup;
    }

    public StudyVariantEngineConfiguration setSetup(VariantSetupResult setup) {
        this.setup = setup;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyVariantEngineConfiguration{");
        sb.append("options=").append(options != null ? options.toJson() : "");
        sb.append(", sampleIndex=").append(sampleIndex);
        sb.append(", setup=").append(setup);
        sb.append('}');
        return sb.toString();
    }
}
