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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.ArrayList;
import java.util.List;

public class ClinicalPipelineExecuteParams {

    @DataField(id = "input", description = FieldConstants.CLINICAL_PIPELINE_INPUT_DESCRIPTION)
    private List<String> input;

    @DataField(id = "indexDir", description = FieldConstants.CLINICAL_PIPELINE_INDEX_DIR_DESCRIPTION)
    private String indexDir;

    @DataField(id = "steps", description = FieldConstants.CLINICAL_PIPELINE_STEPS_DESCRIPTION)
    private List<String> steps;

    @DataField(id = "pipelineFile", description = FieldConstants.CLINICAL_PIPELINE_FILE_DESCRIPTION)
    private String pipelineFile;

    @DataField(id = "pipeline", description = FieldConstants.CLINICAL_PIPELINE_PIPELINE_DESCRIPTION)
    private ObjectMap pipeline;

    public ClinicalPipelineExecuteParams() {
        this.input = new ArrayList<>();
        this.steps = new ArrayList<>();
        pipeline = new ObjectMap();
    }

    public ClinicalPipelineExecuteParams(List<String> input, String indexDir, List<String> steps, String pipelineFile, ObjectMap pipeline) {
        this.input = input;
        this.indexDir = indexDir;
        this.steps = steps;
        this.pipelineFile = pipelineFile;
        this.pipeline = pipeline;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalPipelineExecuteParams{");
        sb.append("input=").append(input);
        sb.append(", indexDir='").append(indexDir).append('\'');
        sb.append(", steps=").append(steps);
        sb.append(", pipelineFile='").append(pipelineFile).append('\'');
        sb.append(", pipeline=").append(pipeline);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getInput() {
        return input;
    }

    public ClinicalPipelineExecuteParams setInput(List<String> input) {
        this.input = input;
        return this;
    }

    public String getIndexDir() {
        return indexDir;
    }

    public ClinicalPipelineExecuteParams setIndexDir(String indexDir) {
        this.indexDir = indexDir;
        return this;
    }

    public List<String> getSteps() {
        return steps;
    }

    public ClinicalPipelineExecuteParams setSteps(List<String> steps) {
        this.steps = steps;
        return this;
    }

    public String getPipelineFile() {
        return pipelineFile;
    }

    public ClinicalPipelineExecuteParams setPipelineFile(String pipelineFile) {
        this.pipelineFile = pipelineFile;
        return this;
    }

    public ObjectMap getPipeline() {
        return pipeline;
    }

    public ClinicalPipelineExecuteParams setPipeline(ObjectMap pipeline) {
        this.pipeline = pipeline;
        return this;
    }
}


