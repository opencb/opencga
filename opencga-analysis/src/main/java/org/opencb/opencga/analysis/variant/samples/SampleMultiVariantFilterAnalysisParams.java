package org.opencb.opencga.analysis.variant.samples;

import org.opencb.opencga.core.tools.ToolParams;

public class SampleMultiVariantFilterAnalysisParams extends ToolParams {

    private String query;

    public String getQuery() {
        return query;
    }

    public SampleMultiVariantFilterAnalysisParams setQuery(String query) {
        this.query = query;
        return this;
    }
}
