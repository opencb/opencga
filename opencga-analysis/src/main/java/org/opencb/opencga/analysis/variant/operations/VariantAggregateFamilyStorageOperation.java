package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.util.List;

@Tool(id = VariantAggregateFamilyStorageOperation.ID, type = Tool.ToolType.VARIANT)
public class VariantAggregateFamilyStorageOperation extends StorageOperation {
    public static final String ID = "variant-aggregate-family";
    private String study;
    private List<String> samples;

    public VariantAggregateFamilyStorageOperation setStudy(String study) {
        this.study = study;
        return this;
    }

    public VariantAggregateFamilyStorageOperation setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        if (samples == null || samples.size() < 2) {
            throw new IllegalArgumentException("Fill gaps operation requires at least two samples!");
        }

        study = getStudyFqn(study);
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            VariantStorageEngine variantStorageEngine = getVariantStorageEngine(study);
            variantStorageEngine.aggregateFamily(study, samples, params);
        });
    }
}
