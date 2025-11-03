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
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class PipelineInput extends ToolParams {
    @DataField(id = "indexDir", description = "Directory containing pipeline indexes")
    private String indexDir;

    @DataField(id = "samples", description = "List of samples to process")
    private List<PipelineSample> samples;

    public PipelineInput() {
    }

    public PipelineInput(String indexDir, List<PipelineSample> samples) {
        this.indexDir = indexDir;
        this.samples = samples;
    }

    public String getIndexDir() { return indexDir; }
    public PipelineInput setIndexDir(String indexDir) { this.indexDir = indexDir; return this; }

    public List<PipelineSample> getSamples() { return samples; }
    public PipelineInput setSamples(List<PipelineSample> samples) { this.samples = samples; return this; }

    @Override
    public String toString() {
        return "PipelineInput{" +
                "indexDir='" + indexDir + '\'' +
                ", samples=" + samples +
                '}';
    }
}