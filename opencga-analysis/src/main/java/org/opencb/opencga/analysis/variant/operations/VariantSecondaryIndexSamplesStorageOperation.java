package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.util.List;

@Tool(id = VariantSecondaryIndexSamplesStorageOperation.ID, type = Tool.ToolType.VARIANT)
public class VariantSecondaryIndexSamplesStorageOperation extends StorageOperation {

    public static final String ID = "variant-search-index-samples";
    private String study;
    private List<String> samples;

    public VariantSecondaryIndexSamplesStorageOperation setStudy(String study) {
        this.study = study;
        return this;
    }

    public VariantSecondaryIndexSamplesStorageOperation setSamples(List<String> samples) {
        this.samples = samples;
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
            variantStorageEngine.getOptions().putAll(params);

            variantStorageEngine.secondaryIndexSamples(study, samples);
        });
    }
}
