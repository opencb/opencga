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

package org.opencb.opencga.core.models.clinical;

import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.ArrayList;
import java.util.List;

public class NgsPipelineWrapperParams extends ToolParams {

    public static final String DESCRIPTION = "Variant caller pipeline parameters";

    @DataField(id = "command", description = FieldConstants.VARIANT_CALLER_PIPELINE_COMMAND_DESCRIPTION)
    private String command;

    @DataField(id = "input", description = FieldConstants.VARIANT_CALLER_PIPELINE_INPUT_DESCRIPTION)
    private List<String> input;

    @DataField(id = "indexDir", description = FieldConstants.VARIANT_CALLER_PIPELINE_INDEX_DIR_DESCRIPTION)
    private String indexDir;

    @DataField(id = "pipelineParams", description = FieldConstants.VARIANT_CALLER_PIPELINE_PARAMS_DESCRIPTION)
    private ObjectMap pipelineParams;

    //    command: "{prepare | run}"
//    outdir: "/data/index"
//    pipelineParams: {
//        ObjectMap pipeline: {}
//        input: ["file://", "file://"]
//        index: ""
//        //steps: ""
//        resume: false
//        overwrite: false
//        outdir:
//
//
    @DataField(id = "outDir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outDir;

    public NgsPipelineWrapperParams() {
        this.input = new ArrayList<>();
    }

    public NgsPipelineWrapperParams(String command, List<String> input, String indexDir, ObjectMap pipelineParams,
                                    String outDir) {
        this.command = command;
        this.input = input;
        this.indexDir = indexDir;
        this.pipelineParams = pipelineParams;
        this.outDir = outDir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantCallerPipelineWrapperParams{");
        sb.append("command='").append(command).append('\'');
        sb.append(", input=").append(input);
        sb.append(", indexDir='").append(indexDir).append('\'');
        sb.append(", pipelineParams=").append(pipelineParams);
        sb.append(", outDir='").append(outDir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getCommand() {
        return command;
    }

    public NgsPipelineWrapperParams setCommand(String command) {
        this.command = command;
        return this;
    }

    public List<String> getInput() {
        return input;
    }

    public NgsPipelineWrapperParams setInput(List<String> input) {
        this.input = input;
        return this;
    }

    public String getIndexDir() {
        return indexDir;
    }

    public NgsPipelineWrapperParams setIndexDir(String indexDir) {
        this.indexDir = indexDir;
        return this;
    }

    public ObjectMap getPipelineParams() {
        return pipelineParams;
    }

    public NgsPipelineWrapperParams setPipelineParams(ObjectMap pipelineParams) {
        this.pipelineParams = pipelineParams;
        return this;
    }

    public String getOutDir() {
        return outDir;
    }

    public NgsPipelineWrapperParams setOutDir(String outDir) {
        this.outDir = outDir;
        return this;
    }
}


