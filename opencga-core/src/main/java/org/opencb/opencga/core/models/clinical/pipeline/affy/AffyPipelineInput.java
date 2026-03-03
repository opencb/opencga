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
import org.opencb.opencga.core.models.clinical.pipeline.PipelineSample;

import java.util.List;

public class AffyPipelineInput {

    @DataField(id = "chip", description = "Chip type used in the Affymetrix experiment")
    protected String chip;

    @DataField(id = "indexDir", description = "Directory containing Affymetrix pipeline indexes")
    private String indexDir;

    @DataField(id = "dataDir", description = "Directory containing Affymetrix pipeline data (e.g., CEL files,...)")
    private String dataDir;


    public AffyPipelineInput() {
    }

    public AffyPipelineInput(String chip, String indexDir, String dataDir) {
        this.chip = chip;
        this.indexDir = indexDir;
        this.dataDir = dataDir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AffyPipelineInput{");
        sb.append("chip='").append(chip).append('\'');
        sb.append(", indexDir='").append(indexDir).append('\'');
        sb.append(", dataDir='").append(dataDir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getChip() {
        return chip;
    }

    public AffyPipelineInput setChip(String chip) {
        this.chip = chip;
        return this;
    }

    public String getIndexDir() {
        return indexDir;
    }

    public AffyPipelineInput setIndexDir(String indexDir) {
        this.indexDir = indexDir;
        return this;
    }

    public String getDataDir() {
        return dataDir;
    }

    public AffyPipelineInput setDataDir(String dataDir) {
        this.dataDir = dataDir;
        return this;
    }
}