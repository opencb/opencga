package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

public class VariantPruneParams extends ToolParams {

    public static final String DESCRIPTION = "";
    private String project;
    private boolean dryRun;
    private boolean resume;


    public String getProject() {
        return project;
    }

    public VariantPruneParams setProject(String project) {
        this.project = project;
        return this;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public VariantPruneParams setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
        return this;
    }

    public boolean isResume() {
        return resume;
    }

    public VariantPruneParams setResume(boolean resume) {
        this.resume = resume;
        return this;
    }
}
