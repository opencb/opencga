package org.opencb.opencga.core.tools.migration.v2_0_0;

import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

public abstract class VariantStorage200MigrationToolExecutor extends OpenCgaToolExecutor {

    private VariantStorage200MigrationToolParams toolParams;

    public VariantStorage200MigrationToolExecutor setToolParams(VariantStorage200MigrationToolParams toolParams) {
        this.toolParams = toolParams;
        return this;
    }

    public VariantStorage200MigrationToolParams getToolParams() {
        return toolParams;
    }
}
