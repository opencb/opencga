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

import java.util.List;

public class PipelineInput {

    @DataField(id = "dataDir", description = "Directory containing data pipeline (e.g., CEL files in affy-pipeline,...)")
    private String dataDir;

    @DataField(id = "indexDir", description = "Directory containing pipeline indexes")
    private String indexDir;

    @DataField(id = "samples", description = "List of samples to process")
    private List<PipelineSample> samples;

    public PipelineInput() {
    }

    public PipelineInput(String dataDir, String indexDir, List<PipelineSample> samples) {
        this.dataDir = dataDir;
        this.indexDir = indexDir;
        this.samples = samples;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineInput{");
        sb.append("dataDir='").append(dataDir).append('\'');
        sb.append(", indexDir='").append(indexDir).append('\'');
        sb.append(", samples=").append(samples);
        sb.append('}');
        return sb.toString();
    }

    public String getDataDir() {
        return dataDir;
    }

    public PipelineInput setDataDir(String dataDir) {
        this.dataDir = dataDir;
        return this;
    }

    public String getIndexDir() {
        return indexDir;
    }

    public PipelineInput setIndexDir(String indexDir) {
        this.indexDir = indexDir;
        return this;
    }

    public List<PipelineSample> getSamples() {
        return samples;
    }

    public PipelineInput setSamples(List<PipelineSample> samples) {
        this.samples = samples;
        return this;
    }
}