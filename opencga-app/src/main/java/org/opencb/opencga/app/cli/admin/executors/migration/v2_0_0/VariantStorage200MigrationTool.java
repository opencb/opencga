package org.opencb.opencga.app.cli.admin.executors.migration.v2_0_0;

import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.migration.v2_0_0.VariantStorage200MigrationToolExecutor;
import org.opencb.opencga.core.tools.migration.v2_0_0.VariantStorage200MigrationToolParams;

@Tool(id = VariantStorage200MigrationTool.ID,
        resource = Enums.Resource.VARIANT,
        type = Tool.Type.OPERATION,
        scope = Tool.Scope.PROJECT,
        description = VariantStorage200MigrationTool.DESCRIPTION)
public class VariantStorage200MigrationTool extends OpenCgaTool {

    public static final String ID = "variant-storage-migration-2.0.0";
    public static final String DESCRIPTION = "Migrate variant storage from v1.4.0 to v2.0.0";

    @ToolParams
    protected VariantStorage200MigrationToolParams toolParams;

    @Override
    protected void check() throws Exception {
        super.check();

        String userId = getCatalogManager().getUserManager().getUserId(getToken());
        if (!userId.equals(ParamConstants.OPENCGA_USER_ID)) {
            throw new CatalogAuthenticationException("Only user '" + ParamConstants.OPENCGA_USER_ID + "' can run this operation!");
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            setUpStorageEngineExecutorByProjectId(toolParams.getProject());
            getToolExecutor(VariantStorage200MigrationToolExecutor.class)
                    .setToolParams(toolParams)
                    .execute();
        });
    }
}
