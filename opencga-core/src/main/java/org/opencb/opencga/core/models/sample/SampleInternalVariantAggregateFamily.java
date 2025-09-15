package org.opencb.opencga.core.models.sample;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.common.IndexStatus;

import java.util.List;

public class SampleInternalVariantAggregateFamily {

    @DataField(description = "Status of the aggregate family.")
    private IndexStatus status;
    @DataField(description = "List of samples that were used to generate the aggregate family. This list might not be exhaustive, "
            + "as it is possible that some samples were not used to generate the aggregate family, but are still part of the family.")
    private List<String> sampleIds;

    public SampleInternalVariantAggregateFamily() {
    }

    public SampleInternalVariantAggregateFamily(IndexStatus status, List<String> sampleIds) {
        this.status = status;
        this.sampleIds = sampleIds;
    }

    public IndexStatus getStatus() {
        return status;
    }

    public SampleInternalVariantAggregateFamily setStatus(IndexStatus status) {
        this.status = status;
        return this;
    }

    public List<String> getSampleIds() {
        return sampleIds;
    }

    public SampleInternalVariantAggregateFamily setSampleIds(List<String> sampleIds) {
        this.sampleIds = sampleIds;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleInternalVariantAggregateFamily{");
        sb.append("status=").append(status);
        sb.append(", sampleIds=").append(sampleIds);
        sb.append('}');
        return sb.toString();
    }
}
