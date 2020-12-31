package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.VariantStorageMetadataSynchronizeParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

@Tool(id = VariantStorageMetadataSynchronizeOperationTool.ID, resource = Enums.Resource.VARIANT, type = Tool.Type.OPERATION,
        scope = Tool.Scope.STUDY, description = VariantStorageMetadataSynchronizeOperationTool.DESCRIPTION)
public class VariantStorageMetadataSynchronizeOperationTool extends OperationTool {
    public static final String ID = "variant-storage-metadata-synchronize";
    public static final String DESCRIPTION = "Synchronize catalog with variant storage metadata";

    @ToolParams
    protected final VariantStorageMetadataSynchronizeParams toolParams = new VariantStorageMetadataSynchronizeParams();

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
        step(()-> {
            Boolean modified = getVariantStorageManager()
                    .synchronizeCatalogStudyFromStorage(toolParams.getStudy(), toolParams.getFiles(), getToken());
            addAttribute("modified", modified);
        });
    }
}
