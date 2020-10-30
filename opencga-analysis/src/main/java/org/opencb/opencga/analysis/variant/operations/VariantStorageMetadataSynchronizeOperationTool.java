package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.VariantFileIndexJobLauncherParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

@Tool(id = VariantStorageMetadataSynchronizeOperationTool.ID, resource = Enums.Resource.VARIANT, type = Tool.Type.OPERATION,
        scope = Tool.Scope.STUDY, description = VariantStorageMetadataSynchronizeOperationTool.DESCRIPTION)
public class VariantStorageMetadataSynchronizeOperationTool extends OperationTool {
    public static final String ID = "variant-storage-metadata-synchronize";
    public static final String DESCRIPTION = "Synchronize catalog with variant storage metadata";

    @ToolParams
    protected final VariantFileIndexJobLauncherParams toolParams = new VariantFileIndexJobLauncherParams();

    @Override
    protected void run() throws Exception {
        step(()-> {
            Boolean modified = getVariantStorageManager().synchronizeCatalogStudyFromStorage(getStudyFqn(), getToken());
            addAttribute("modified", modified);
        });
    }
}
