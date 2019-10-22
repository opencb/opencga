package org.opencb.opencga.core.analysis.variant;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.analysis.OpenCgaAnalysisExecutor;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.analysis.result.FileResult;

import java.nio.file.Path;
import java.util.List;

public abstract class SampleVariantStatsAnalysisExecutor extends OpenCgaAnalysisExecutor {

    protected String study;
    protected List<String> sampleNames;
    protected String individualId;
    protected String familyId;
    private Path outputFile;

    public SampleVariantStatsAnalysisExecutor() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleStatsExecutor{");
        sb.append("sampleNames=").append(sampleNames);
        sb.append(", individualId='").append(individualId).append('\'');
        sb.append(", familyId='").append(familyId).append('\'');
        sb.append(", executorParams=").append(executorParams);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }

    public String getStudy() {
        return study;
    }

    public SampleVariantStatsAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSampleNames() {
        return sampleNames;
    }

    public SampleVariantStatsAnalysisExecutor setSampleNames(List<String> sampleNames) {
        this.sampleNames = sampleNames;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public SampleVariantStatsAnalysisExecutor setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getFamilyId() {
        return familyId;
    }

    public SampleVariantStatsAnalysisExecutor setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public Path getOutputFile() {
        return outputFile;
    }

    public SampleVariantStatsAnalysisExecutor setOutputFile(Path outputFile) {
        this.outputFile = outputFile;
        return this;
    }

    protected void writeStatsToFile(List<SampleVariantStats> stats) throws AnalysisException {
        ObjectMapper objectMapper = new ObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        ObjectWriter objectWriter = objectMapper.writer().withDefaultPrettyPrinter();

        Path outFilename = getOutputFile();
        try {
            objectWriter.writeValue(outFilename.toFile(), stats);
        } catch (Exception e) {
            throw new AnalysisException("Error writing output file: " + outFilename, e);
        }
    }
}
