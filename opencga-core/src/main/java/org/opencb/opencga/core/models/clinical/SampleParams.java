package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.models.sample.Sample;

public class SampleParams {
    private String id;

    public SampleParams() {
    }

    public SampleParams(String id) {
        this.id = id;
    }

    public static SampleParams of(Sample sample) {
        return new SampleParams(sample.getId());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleParams{");
        sb.append("id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public SampleParams setId(String id) {
        this.id = id;
        return this;
    }
}
