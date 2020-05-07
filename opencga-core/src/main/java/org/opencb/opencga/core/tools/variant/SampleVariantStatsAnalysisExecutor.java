/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.tools.variant;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;

import java.nio.file.Path;
import java.util.List;

public abstract class SampleVariantStatsAnalysisExecutor extends OpenCgaToolExecutor {

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
        sb.append(", executorParams=").append(getExecutorParams());
        sb.append(", outDir=").append(getOutDir());
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

    protected void writeStatsToFile(List<SampleVariantStats> stats) throws ToolException {
        ObjectMapper objectMapper = new ObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        ObjectWriter objectWriter = objectMapper.writer().withDefaultPrettyPrinter();

        Path outFilename = getOutputFile();
        try {
            objectWriter.writeValue(outFilename.toFile(), stats);
        } catch (Exception e) {
            throw new ToolException("Error writing output file: " + outFilename, e);
        }
    }
}
