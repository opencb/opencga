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

package org.opencb.opencga.core.models.clinical.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.operations.variant.VariantIndexParams;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AffyClinicalPipelineParams extends ToolParams {

    @DataField(id = "samples", description = FieldConstants.CLINICAL_PIPELINE_SAMPLES_DESCRIPTION)
    private List<String> samples;

    @DataField(id = "indexDir", description = FieldConstants.CLINICAL_PIPELINE_INDEX_DIR_DESCRIPTION)
    private String indexDir;

    @DataField(id = "dataDir", description = FieldConstants.CLINICAL_PIPELINE_DATA_DIR_DESCRIPTION)
    private String dataDir;

    @DataField(id = "steps", description = FieldConstants.CLINICAL_PIPELINE_STEPS_DESCRIPTION)
    private List<String> steps;

    @DataField(id = "variantIndexParams", description = FieldConstants.CLINICAL_PIPELINE_VARIANT_INDEX_DESCRIPTION)
    private VariantIndexParams variantIndexParams;

    @DataField(id = "pipelineFile", description = FieldConstants.CLINICAL_PIPELINE_FILE_DESCRIPTION)
    private String pipelineFile;

    @DataField(id = "pipeline", description = FieldConstants.CLINICAL_PIPELINE_PIPELINE_DESCRIPTION)
    private AffyPipelineConfig pipeline;

    public AffyClinicalPipelineParams() {
        this.samples = new ArrayList<>();
        this.steps = new ArrayList<>();
    }

    public AffyClinicalPipelineParams(List<String> samples, String indexDir, List<String> steps, VariantIndexParams variantIndexParams,
                                      String pipelineFile, AffyPipelineConfig pipeline) {
        this.samples = samples;
        this.indexDir = indexDir;
        this.steps = steps;
        this.variantIndexParams = variantIndexParams;
        this.pipelineFile = pipelineFile;
        this.pipeline = pipeline;
    }

    public AffyClinicalPipelineParams(String input) throws JsonProcessingException {
        // Construct this from a JSON string
        AffyClinicalPipelineParams params = JacksonUtils.getDefaultObjectMapper().readerFor(AffyClinicalPipelineParams.class)
                .readValue(input);
        this.samples = params.samples;
        this.indexDir = params.indexDir;
        this.steps = params.steps;
        this.variantIndexParams = params.variantIndexParams;
        this.pipelineFile = params.pipelineFile;
        this.pipeline = params.pipeline;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AffyClinicalPipelineParams{");
        sb.append("samples=").append(samples);
        sb.append(", indexDir='").append(indexDir).append('\'');
        sb.append(", steps=").append(steps);
        sb.append(", variantIndexParams=").append(variantIndexParams);
        sb.append(", pipelineFile='").append(pipelineFile).append('\'');
        sb.append(", pipeline=").append(pipeline);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getSamples() {
        return samples;
    }

    public AffyClinicalPipelineParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public String getIndexDir() {
        return indexDir;
    }

    public AffyClinicalPipelineParams setIndexDir(String indexDir) {
        this.indexDir = indexDir;
        return this;
    }

    public List<String> getSteps() {
        return steps;
    }

    public AffyClinicalPipelineParams setSteps(List<String> steps) {
        this.steps = steps;
        return this;
    }

    public VariantIndexParams getVariantIndexParams() {
        return variantIndexParams;
    }

    public AffyClinicalPipelineParams setVariantIndexParams(VariantIndexParams variantIndexParams) {
        this.variantIndexParams = variantIndexParams;
        return this;
    }

    public String getPipelineFile() {
        return pipelineFile;
    }

    public AffyClinicalPipelineParams setPipelineFile(String pipelineFile) {
        this.pipelineFile = pipelineFile;
        return this;
    }

    public AffyPipelineConfig getPipeline() {
        return pipeline;
    }

    public AffyClinicalPipelineParams setPipeline(AffyPipelineConfig pipeline) {
        this.pipeline = pipeline;
        return this;
    }
}


