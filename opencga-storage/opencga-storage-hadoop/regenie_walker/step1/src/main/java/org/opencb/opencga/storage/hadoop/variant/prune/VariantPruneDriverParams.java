package org.opencb.opencga.storage.hadoop.variant.prune;

import org.opencb.opencga.core.tools.ToolParams;

public class VariantPruneDriverParams extends ToolParams {

    private boolean dryRun;
    private String output;

    public boolean isDryRun() {
        return dryRun;
    }

    public VariantPruneDriverParams setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
        return this;
    }

    public String getOutput() {
        return output;
    }

    public VariantPruneDriverParams setOutput(String output) {
        this.output = output;
        return this;
    }
}
