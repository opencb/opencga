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

import java.util.List;

public class AffyClinicalPipelineParams extends ClinicalPipelineParams<AffyPipelineConfig> {

    @DataField(id = "dataDir", description = "Directory where the data files are located, e.g. CEL files in affy pipelines")
    protected String dataDir;

    public AffyClinicalPipelineParams() {
        super();
    }

    public AffyClinicalPipelineParams(String dataDir) {
        this.dataDir = dataDir;
    }

    public AffyClinicalPipelineParams(String indexDir, List<String> steps, VariantIndexParams variantIndexParams, String pipelineFile,
                                      AffyPipelineConfig pipeline, String dataDir) {
        super(indexDir, steps, variantIndexParams, pipelineFile, pipeline);
        this.dataDir = dataDir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AffyClinicalPipelineParams{");
        sb.append("dataDir='").append(dataDir).append('\'');
        sb.append(", indexDir='").append(indexDir).append('\'');
        sb.append(", steps=").append(steps);
        sb.append(", variantIndexParams=").append(variantIndexParams);
        sb.append(", pipelineFile='").append(pipelineFile).append('\'');
        sb.append(", pipeline=").append(pipeline);
        sb.append('}');
        return sb.toString();
    }

    public String getDataDir() {
        return dataDir;
    }

    public AffyClinicalPipelineParams setDataDir(String dataDir) {
        this.dataDir = dataDir;
        return this;
    }
}


