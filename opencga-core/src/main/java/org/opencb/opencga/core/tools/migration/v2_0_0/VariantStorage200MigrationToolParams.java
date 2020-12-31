package org.opencb.opencga.core.tools.migration.v2_0_0;

import org.opencb.opencga.core.tools.ToolParams;

public class VariantStorage200MigrationToolParams extends ToolParams {

    private String project;
    private boolean forceMigration;
    private boolean removeSpanDeletions;
    private String region;


    public String getProject() {
        return project;
    }

    public VariantStorage200MigrationToolParams setProject(String project) {
        this.project = project;
        return this;
    }

    public boolean isForceMigration() {
        return forceMigration;
    }

    public VariantStorage200MigrationToolParams setForceMigration(boolean forceMigration) {
        this.forceMigration = forceMigration;
        return this;
    }

    public boolean isRemoveSpanDeletions() {
        return removeSpanDeletions;
    }

    public VariantStorage200MigrationToolParams setRemoveSpanDeletions(boolean removeSpanDeletions) {
        this.removeSpanDeletions = removeSpanDeletions;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public VariantStorage200MigrationToolParams setRegion(String region) {
        this.region = region;
        return this;
    }
}
