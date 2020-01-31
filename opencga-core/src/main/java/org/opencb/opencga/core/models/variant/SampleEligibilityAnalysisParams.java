package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

public class SampleEligibilityAnalysisParams extends ToolParams {

    public static final String DESCRIPTION = "";
    private String query;
    private boolean index;
    private String cohortId;

    public SampleEligibilityAnalysisParams() {
    }

    public SampleEligibilityAnalysisParams(String query, boolean index, String cohortId) {
        this.query = query;
        this.index = index;
        this.cohortId = cohortId;
    }

    public String getQuery() {
        return query;
    }

    public SampleEligibilityAnalysisParams setQuery(String query) {
        this.query = query;
        return this;
    }

    public boolean isIndex() {
        return index;
    }

    public SampleEligibilityAnalysisParams setIndex(boolean index) {
        this.index = index;
        return this;
    }

    public String getCohortId() {
        return cohortId;
    }

    public SampleEligibilityAnalysisParams setCohortId(String cohortId) {
        this.cohortId = cohortId;
        return this;
    }
}
