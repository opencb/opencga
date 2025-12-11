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

package org.opencb.opencga.core.models.clinical.pipeline.affy;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.clinical.pipeline.ClinicalPipelineParams;
import org.opencb.opencga.core.models.operations.variant.VariantIndexParams;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class AffyClinicalPipelineParams extends ToolParams {

    @DataField(id = "chip", description = "Chip type used in the Affymetrix experiment")
    protected String chip;

    @DataField(id = "indexDir", description = "Directory containing Affymetrix pipeline indexes")
    private String indexDir;

    @DataField(id = "dataDir", description = "Directory containing Affymetrix pipeline data (e.g., CEL files,...)")
    private String dataDir;

    @DataField(id = "variantIndexParams", description = "Parameters to index the resulting variants in OpenCGA storage")
    protected VariantIndexParams variantIndexParams;

    @DataField(id = "pipelineFile", description = "Affymetrix pipeline configuration file")
    protected String pipelineFile;

    @DataField(id = "pipeline", description = "Affymetrix pipeline configuration")
    protected AffyPipelineConfig pipeline;

    public AffyClinicalPipelineParams() {
    }

    public AffyClinicalPipelineParams(String chip, String indexDir, String dataDir, VariantIndexParams variantIndexParams,
                                      String pipelineFile, AffyPipelineConfig pipeline) {
        this.chip = chip;
        this.indexDir = indexDir;
        this.dataDir = dataDir;
        this.variantIndexParams = variantIndexParams;
        this.pipelineFile = pipelineFile;
        this.pipeline = pipeline;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AffyClinicalPipelineParams{");
        sb.append("chip='").append(chip).append('\'');
        sb.append(", indexDir='").append(indexDir).append('\'');
        sb.append(", dataDir='").append(dataDir).append('\'');
        sb.append(", variantIndexParams=").append(variantIndexParams);
        sb.append(", pipelineFile='").append(pipelineFile).append('\'');
        sb.append(", pipeline=").append(pipeline);
        sb.append('}');
        return sb.toString();
    }

    public String getChip() {
        return chip;
    }

    public AffyClinicalPipelineParams setChip(String chip) {
        this.chip = chip;
        return this;
    }

    public String getIndexDir() {
        return indexDir;
    }

    public AffyClinicalPipelineParams setIndexDir(String indexDir) {
        this.indexDir = indexDir;
        return this;
    }

    public String getDataDir() {
        return dataDir;
    }

    public AffyClinicalPipelineParams setDataDir(String dataDir) {
        this.dataDir = dataDir;
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


