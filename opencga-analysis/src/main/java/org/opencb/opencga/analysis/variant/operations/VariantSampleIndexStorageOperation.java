package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.opencga.core.annotations.Analysis;

import java.util.ArrayList;
import java.util.List;

@Analysis(id = "variant-sample-index", type = Analysis.AnalysisType.VARIANT)
public class VariantSampleIndexStorageOperation extends StorageOperation {

    protected String study;
    protected List<String> samples;
    protected boolean buildIndex;
    protected boolean annotate;

    public VariantSampleIndexStorageOperation setStudy(String study) {
        this.study = study;
        return this;
    }

    public VariantSampleIndexStorageOperation setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public VariantSampleIndexStorageOperation setBuildIndex(boolean buildIndex) {
        this.buildIndex = buildIndex;
        return this;
    }

    public VariantSampleIndexStorageOperation setAnnotate(boolean annotate) {
        this.annotate = annotate;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        study = getStudyFqn(study);

        if (CollectionUtils.isEmpty(samples)) {
            throw new IllegalArgumentException("Empty list of samples");
        }
        if (!buildIndex && !annotate) {
            buildIndex = true;
            annotate = true;
        }
    }

    @Override
    protected List<String> getSteps() {
        ArrayList<String> steps = new ArrayList<>();
        if (buildIndex) {
            steps.add("buildIndex");
        } else if (annotate) {
            steps.add("annotate");
        }
        return steps;
    }

    @Override
    protected void run() throws Exception {
        if (buildIndex) {
            step("buildIndex", () -> getVariantStorageEngine(study).sampleIndex(study, samples, params));
        }
        if (annotate) {
            step("annotate", () -> getVariantStorageEngine(study).sampleIndexAnnotate(study, samples, params));
        }
    }
}
