package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.annotations.Tool;

@Tool(id = VariantSecondaryIndexSamplesDeleteStorageOperation.ID, type = Tool.ToolType.VARIANT)
public class VariantSecondaryIndexSamplesDeleteStorageOperation extends StorageOperation {

    public static final String ID = "variant-secondary-index-samples-delete";

    @Override
    protected void run() throws Exception {

    }
}
