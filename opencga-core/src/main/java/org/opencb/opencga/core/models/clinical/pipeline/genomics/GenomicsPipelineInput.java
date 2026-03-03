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

package org.opencb.opencga.core.models.clinical.pipeline.genomics;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineSample;

import java.util.List;

public class GenomicsPipelineInput {

    @DataField(id = "samples", description = "List of samples to process")
    private List<PipelineSample> samples;

    @DataField(id = "indexDir", description = "Directory containing genomics pipeline indexes")
    private String indexDir;

    public GenomicsPipelineInput() {
    }

    public GenomicsPipelineInput(List<PipelineSample> samples, String indexDir) {
        this.samples = samples;
        this.indexDir = indexDir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GenomicsPipelineInput{");
        sb.append("samples=").append(samples);
        sb.append(", indexDir='").append(indexDir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public List<PipelineSample> getSamples() {
        return samples;
    }

    public GenomicsPipelineInput setSamples(List<PipelineSample> samples) {
        this.samples = samples;
        return this;
    }

    public String getIndexDir() {
        return indexDir;
    }

    public GenomicsPipelineInput setIndexDir(String indexDir) {
        this.indexDir = indexDir;
        return this;
    }
}