package org.opencb.opencga.core.analysis.variant;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.analysis.OpenCgaAnalysisExecutor;
import org.opencb.opencga.core.exception.AnalysisExecutorException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public abstract class VariantStatsAnalysisExecutor extends OpenCgaAnalysisExecutor {

    private Path outputFile;
    private String study;
    private Map<String, List<String>> cohorts;
    private Query variantsQuery;
    private boolean index;

    public VariantStatsAnalysisExecutor() {
    }

    public String getStudy() {
        return study;
    }

    public VariantStatsAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Map<String, List<String>> getCohorts() {
        return cohorts;
    }

    public VariantStatsAnalysisExecutor setCohorts(Map<String, List<String>> cohorts) {
        this.cohorts = cohorts;
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

    public boolean isIndex() {
        return index;
    }

    public VariantStatsAnalysisExecutor setIndex(boolean index) {
        this.index = index;
        return this;
    }
}
