package org.opencb.opencga.core.models.alignment;

import org.opencb.biodata.models.alignment.GeneCoverageStats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CoverageFileQualityControl implements Serializable {

    private List<GeneCoverageStats> geneCoverageStats;

    public CoverageFileQualityControl() {
        this(new ArrayList<>());
    }

    public CoverageFileQualityControl(List<GeneCoverageStats> geneCoverageStats) {
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

    public CoverageFileQualityControl setGeneCoverageStats(List<GeneCoverageStats> geneCoverageStats) {
        this.geneCoverageStats = geneCoverageStats;
        return this;
    }
}
