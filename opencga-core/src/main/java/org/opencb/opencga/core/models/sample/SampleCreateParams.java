package org.opencb.opencga.core.models.sample;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class SampleCreateParams extends SampleUpdateParams {

    @Deprecated
    public String name;
    @Deprecated
    public Map<String, Object> stats;

    public SampleCreateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleCreateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", stats=").append(stats);
        sb.append(", id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", individualId='").append(individualId).append('\'');
        sb.append(", processing=").append(processing);
        sb.append(", collection=").append(collection);
        sb.append(", source='").append(source).append('\'');
        sb.append(", somatic=").append(somatic);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public Sample toSample() {

        String sampleId = StringUtils.isEmpty(this.getId()) ? name : this.getId();
        String sampleName = StringUtils.isEmpty(name) ? sampleId : name;
        return new Sample(sampleId, getSource(), getIndividualId(), getProcessing(), getCollection(), 1, 1, getDescription(), getType(),
                getSomatic(), getPhenotypes(), getAnnotationSets(), getAttributes())
                .setName(sampleName).setStats(stats);
    }

    public String getName() {
        return name;
    }

    public SampleCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public SampleCreateParams setStats(Map<String, Object> stats) {
        this.stats = stats;
        return this;
    }
}
