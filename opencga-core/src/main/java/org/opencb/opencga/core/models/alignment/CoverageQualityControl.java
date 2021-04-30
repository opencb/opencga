package org.opencb.opencga.core.models.alignment;

import org.opencb.biodata.models.alignment.GeneCoverageStats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CoverageQualityControl implements Serializable {
    private List<GeneCoverageStats> geneCoverageStats;

    public CoverageQualityControl() {
        this(new ArrayList<>());
    }

    public CoverageQualityControl(List<GeneCoverageStats> geneCoverageStats) {
        this.geneCoverageStats = geneCoverageStats;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Coverage{");
        sb.append(", geneCoverageStats=").append(geneCoverageStats);
        sb.append('}');
        return sb.toString();
    }

    public List<GeneCoverageStats> getGeneCoverageStats() {
        return geneCoverageStats;
    }

    public CoverageQualityControl setGeneCoverageStats(List<GeneCoverageStats> geneCoverageStats) {
        this.geneCoverageStats = geneCoverageStats;
        return this;
    }
}
