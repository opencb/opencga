package org.opencb.opencga.core.analysis.variant;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.analysis.OpenCgaAnalysisExecutor;
import org.opencb.opencga.core.exception.AnalysisException;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;

public abstract class CohortVariantStatsAnalysisExecutor extends OpenCgaAnalysisExecutor {

    private String study;
    private List<String> sampleNames;
    private Path outputFile;

    public CohortVariantStatsAnalysisExecutor() {
    }

    public CohortVariantStatsAnalysisExecutor(ObjectMap executorParams, Path outDir) {
        this.setUp(executorParams, outDir);
    }

    public String getStudy() {
        return study;
    }

    public CohortVariantStatsAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSampleNames() {
        return sampleNames;
    }

    public CohortVariantStatsAnalysisExecutor setSampleNames(List<String> sampleNames) {
        this.sampleNames = sampleNames;
        return this;
    }

    public Path getOutputFile() {
        return outputFile;
    }

    public CohortVariantStatsAnalysisExecutor setOutputFile(Path outputFile) {
        this.outputFile = outputFile;
        return this;
    }

    protected void writeStatsToFile(VariantSetStats stats) throws AnalysisException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

        File outFilename = getOutputFile().toFile();
        try {
            PrintWriter pw = new PrintWriter(outFilename);
            pw.println(objectMapper.writer().writeValueAsString(stats));
            pw.close();
        } catch (Exception e) {
            throw new AnalysisException("Error writing output file: " + outFilename, e);
        }
    }
}
