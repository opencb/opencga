package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

@Analysis(id = VariantAggregateStorageOperation.ID, type = Analysis.AnalysisType.VARIANT)
public class VariantAggregateStorageOperation extends StorageOperation {

    public static final String ID = "variant-aggregate";
    private String study;
    private boolean overwrite;

    public VariantAggregateStorageOperation setStudy(String study) {
        this.study = study;
        return this;
    }

    public VariantAggregateStorageOperation setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();

        study = getStudyFqn(study);
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            VariantStorageEngine variantStorageEngine = getVariantStorageEngine(study);
            variantStorageEngine.aggregate(study, params, overwrite);
        });
    }
}
