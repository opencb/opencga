package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.annotations.Analysis;

@Analysis(id = VariantSecondaryIndexSamplesDeleteStorageOperation.ID, type = Analysis.AnalysisType.VARIANT)
public class VariantSecondaryIndexSamplesDeleteStorageOperation extends StorageOperation {

    public static final String ID = "variant-secondary-index-samples-delete";

    @Override
    protected void run() throws Exception {

    }
}
