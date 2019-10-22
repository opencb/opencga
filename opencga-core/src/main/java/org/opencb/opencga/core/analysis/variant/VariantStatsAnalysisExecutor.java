package org.opencb.opencga.core.analysis.variant;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.analysis.OpenCgaAnalysisExecutor;

import java.nio.file.Path;
import java.util.List;

public abstract class VariantStatsAnalysisExecutor extends OpenCgaAnalysisExecutor {

    private Path outputFile;
    private String study;
    private String cohort;
    private List<String> samples;
    private Query variantsQuery;

    public VariantStatsAnalysisExecutor() {
    }

    public VariantStatsAnalysisExecutor(ObjectMap executorParams, Path outDir) {
        this(null, executorParams, outDir);
    }

    public VariantStatsAnalysisExecutor(String cohort, ObjectMap executorParams, Path outDir) {
        setUp(executorParams, outDir);
        this.cohort = cohort;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantStatsAnalysisExecutor{");
        sb.append("cohort='").append(cohort).append('\'');
        sb.append(", executorParams=").append(executorParams);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }

    public String getStudy() {
        return study;
    }

    public VariantStatsAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getCohort() {
        return cohort;
    }

    public VariantStatsAnalysisExecutor setCohort(String cohort) {
        this.cohort = cohort;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public VariantStatsAnalysisExecutor setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public Path getOutputFile() {
        return outputFile;
    }

    public VariantStatsAnalysisExecutor setOutputFile(Path outputFile) {
        this.outputFile = outputFile;
        return this;
    }

    public VariantStatsAnalysisExecutor setVariantsQuery(Query variantsQuery) {
        this.variantsQuery = variantsQuery;
        return this;
    }

    public Query getVariantsQuery() {
        return variantsQuery;
    }
}
