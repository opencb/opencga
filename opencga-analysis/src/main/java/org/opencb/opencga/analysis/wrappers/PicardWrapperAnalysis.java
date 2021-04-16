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

package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.wrappers.executors.PicardWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

@Tool(id = PicardWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT,
        description = "")
public class PicardWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "picard";
    public final static String DESCRIPTION = "Picard is a set of command line tools (in Java) for manipulating high-throughput sequencing"
            + " (HTS) data and formats such as SAM/BAM/CRAM and VCF.";

    public final static String PICARD_DOCKER_IMAGE = "broadinstitute/picard";

    private String command;

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(command)) {
            throw new ToolException("Missig Picard command.");
        }

        switch (command) {
            case "CollectHsMetrics":
            case "CollectWgsMetrics":
            case "BedToIntervalList":
                break;
            default:
                throw new ToolException("Picard tool name '" + command + "' is not available. Supported tools: CollectHsMetrics,"
                        + " CollectWgsMetrics, BedToIntervalList");
        }
    }

    @Override
    protected void run() throws Exception {

        step(() -> {
                    PicardWrapperAnalysisExecutor executor = new PicardWrapperAnalysisExecutor(getStudy(), params, getOutDir(),
                            getScratchDir(), catalogManager, token);

                    executor.setCommand(command);
                    executor.run();
                }
        );
    }

//    @Override
//    public String getDockerImageName() {
//        return PICARD_DOCKER_IMAGE;
//    }


    public String getCommand() {
        return command;
    }

    public PicardWrapperAnalysis setCommand(String command) {
        this.command = command;
        return this;
    }
}
