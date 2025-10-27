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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.operations.variant.VariantIndexParams;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.ArrayList;
import java.util.List;

public class ClinicalPipelineGenomicsParams extends ToolParams {

    @DataField(id = "samples", description = FieldConstants.CLINICAL_PIPELINE_SAMPLES_DESCRIPTION)
    private List<String> samples;

    @DataField(id = "indexDir", description = FieldConstants.CLINICAL_PIPELINE_INDEX_DIR_DESCRIPTION)
    private String indexDir;

    @DataField(id = "steps", description = FieldConstants.CLINICAL_PIPELINE_STEPS_DESCRIPTION)
    private List<String> steps;

    @DataField(id = "variantIndexParams", description = FieldConstants.CLINICAL_PIPELINE_VARIANT_INDEX_DESCRIPTION)
    private VariantIndexParams variantIndexParams;

    @DataField(id = "pipelineFile", description = FieldConstants.CLINICAL_PIPELINE_FILE_DESCRIPTION)
    private String pipelineFile;

    @DataField(id = "pipeline", description = FieldConstants.CLINICAL_PIPELINE_PIPELINE_DESCRIPTION)
    private PipelineConfig pipeline;

    public ClinicalPipelineGenomicsParams() {
        this.samples = new ArrayList<>();
        this.steps = new ArrayList<>();
        pipeline = new PipelineConfig();
    }

    public ClinicalPipelineGenomicsParams(List<String> samples, String indexDir, List<String> steps, VariantIndexParams variantIndexParams,
                                          String pipelineFile, PipelineConfig pipeline) {
        this.samples = samples;
        this.indexDir = indexDir;
        this.steps = steps;
        this.variantIndexParams = variantIndexParams;
        this.pipelineFile = pipelineFile;
        this.pipeline = pipeline;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalPipelineExecuteParams{");
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

    public ClinicalPipelineGenomicsParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public String getIndexDir() {
        return indexDir;
    }

    public ClinicalPipelineGenomicsParams setIndexDir(String indexDir) {
        this.indexDir = indexDir;
        return this;
    }

    public List<String> getSteps() {
        return steps;
    }

    public ClinicalPipelineGenomicsParams setSteps(List<String> steps) {
        this.steps = steps;
        return this;
    }

    public VariantIndexParams getVariantIndexParams() {
        return variantIndexParams;
    }

    public ClinicalPipelineGenomicsParams setVariantIndexParams(VariantIndexParams variantIndexParams) {
        this.variantIndexParams = variantIndexParams;
        return this;
    }

    public String getPipelineFile() {
        return pipelineFile;
    }

    public ClinicalPipelineGenomicsParams setPipelineFile(String pipelineFile) {
        this.pipelineFile = pipelineFile;
        return this;
    }

    public PipelineConfig getPipeline() {
        return pipeline;
    }

    public ClinicalPipelineGenomicsParams setPipeline(PipelineConfig pipeline) {
        this.pipeline = pipeline;
        return this;
    }
}


