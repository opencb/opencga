package org.opencb.opencga.core.api.operations.variant;

import org.opencb.opencga.core.tools.ToolParams;

public class VariantAggregateParams extends ToolParams {
    public static final String DESCRIPTION = "Variant aggregate params.";
    //    private String region;
    private boolean overwrite;
    private boolean resume;

    public VariantAggregateParams() {
    }

    public VariantAggregateParams(boolean overwrite, boolean resume) {
        this.overwrite = overwrite;
        this.resume = resume;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public VariantAggregateParams setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public boolean isResume() {
        return resume;
    }

    public VariantAggregateParams setResume(boolean resume) {
        this.resume = resume;
        return this;
    }
}
