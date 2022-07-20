package org.opencb.opencga.core.models.alignment;

import org.opencb.biodata.models.alignment.GeneCoverageStats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class CoverageFileQualityControl implements Serializable {

    @DataField(description = ParamConstants.COVERAGE_FILE_QUALITY_CONTROL_GENE_COVERAGE_STATS_DESCRIPTION)
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
