package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantPruneParams extends ToolParams {

    public static final String DESCRIPTION = "Variant prune params. Use dry-run to just generate a report with the orphan variants.";
    @DataField(description = ParamConstants.VARIANT_PRUNE_PARAMS_PROJECT_DESCRIPTION)
    private String project;
    @DataField(description = ParamConstants.VARIANT_PRUNE_PARAMS_DRY_RUN_DESCRIPTION)
    private boolean dryRun;
    @DataField(description = ParamConstants.VARIANT_PRUNE_PARAMS_RESUME_DESCRIPTION)
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
