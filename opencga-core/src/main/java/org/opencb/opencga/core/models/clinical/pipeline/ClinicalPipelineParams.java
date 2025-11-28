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
import org.opencb.opencga.core.models.operations.variant.VariantIndexParams;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.ArrayList;
import java.util.List;

public class ClinicalPipelineParams<T extends PipelineConfig> extends ToolParams {

    @DataField(id = "indexDir", description = "Directory where the reference genome, aligner indexes are located, and in affy pipelines,"
            + " Affymetrix files too")
    protected String indexDir;

    @DataField(id = "steps", description = "Pipeline steps: quality-control, alignment, variant-calling, genotype,...")
    protected List<String> steps;

    @DataField(id = "variantIndexParams", description = "Parameters to index the resulting variants in OpenCGA storage")
    protected VariantIndexParams variantIndexParams;

    @DataField(id = "pipelineFile", description = "Clinical pipeline configuration file")
    protected String pipelineFile;

    @DataField(id = "pipeline", description = "Clinical pipeline configuration")
    protected T pipeline;

    public ClinicalPipelineParams() {
        this.steps = new ArrayList<>();
    }

    public ClinicalPipelineParams(String indexDir, List<String> steps, VariantIndexParams variantIndexParams, String pipelineFile, T pipeline) {
        this.indexDir = indexDir;
        this.steps = steps;
        this.variantIndexParams = variantIndexParams;
        this.pipelineFile = pipelineFile;
        this.pipeline = pipeline;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalPipelineParams{");
        sb.append("indexDir='").append(indexDir).append('\'');
        sb.append(", steps=").append(steps);
        sb.append(", variantIndexParams=").append(variantIndexParams);
        sb.append(", pipelineFile='").append(pipelineFile).append('\'');
        sb.append(", pipeline=").append(pipeline);
        sb.append('}');
        return sb.toString();
    }

    public String getIndexDir() {
        return indexDir;
    }

    public ClinicalPipelineParams<T> setIndexDir(String indexDir) {
        this.indexDir = indexDir;
        return this;
    }

    public List<String> getSteps() {
        return steps;
    }

    public ClinicalPipelineParams<T> setSteps(List<String> steps) {
        this.steps = steps;
        return this;
    }

    public VariantIndexParams getVariantIndexParams() {
        return variantIndexParams;
    }

    public ClinicalPipelineParams<T> setVariantIndexParams(VariantIndexParams variantIndexParams) {
        this.variantIndexParams = variantIndexParams;
        return this;
    }

    public String getPipelineFile() {
        return pipelineFile;
    }

    public ClinicalPipelineParams<T> setPipelineFile(String pipelineFile) {
        this.pipelineFile = pipelineFile;
        return this;
    }

    public T getPipeline() {
        return pipeline;
    }

    public ClinicalPipelineParams<T> setPipeline(T pipeline) {
        this.pipeline = pipeline;
        return this;
    }
}


