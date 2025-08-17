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

package org.opencb.opencga.analysis.wrappers.hisat2;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.wrapper.hisat2.Hisat2Params;
import org.opencb.opencga.core.models.wrapper.hisat2.Hisat2WrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.checkParams;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.checkPaths;
import static org.opencb.opencga.core.models.wrapper.hisat2.Hisat2Params.*;

@Tool(id = Hisat2WrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = Hisat2WrapperAnalysis.DESCRIPTION)
public class Hisat2WrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "hisat2";
    public final static String DESCRIPTION = "HISAT2 is a fast and sensitive alignment program for mapping next-generation sequencing"
            + " reads (whole-genome, transcriptome, and exome sequencing data) against the general human population.";

    @ToolParams
    protected final Hisat2WrapperParams analysisParams = new Hisat2WrapperParams();

    private Hisat2Params updatedParams = new Hisat2Params();

    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check command
        updatedParams.setCommand(analysisParams.getHisat2Params().getCommand());
        if (StringUtils.isEmpty(updatedParams.getCommand())) {
            updatedParams.setCommand(HISAT2_TOOL);
        }

        switch (updatedParams.getCommand()) {
            case HISAT2_TOOL: {
                checkHisat2ToolParams();
                break;
            }
            case HISAT2_BUILD_TOOL: {
                checkHisat2BuildToolParams();
                break;
            }
            default:
                throw new ToolException("Unsupported HISAT2 command '" + updatedParams.getCommand() + "'. Supported commands are: "
                        + HISAT2_TOOL + ", " + HISAT2_BUILD_TOOL + ".");
        }
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(ID);
    }

    protected void run() throws ToolException, IOException {
        // Run HISAT2
        step(ID, this::runHisat2);
    }

    protected void runHisat2() throws ToolException {
        // Get executor
        Hisat2WrapperAnalysisExecutor executor = getToolExecutor(Hisat2WrapperAnalysisExecutor.class);

        // Set parameters and execute
        executor.setStudy(study)
                .setHisat2WrapperParams(updatedParams)
                .execute();
    }

    private void checkHisat2ToolParams() throws ToolException {
        // Check X_PARAM option, is mandatory
        if (!analysisParams.getHisat2Params().getOptions().containsKey(X_PARAM)) {
            throw new ToolException("Missing mandatory parameter '" + X_PARAM + "'. Please, specify the index directory.");
        }

        // Check S_PARAM option, is mandatory
        if (!analysisParams.getHisat2Params().getOptions().containsKey(S_PARAM)) {
            throw new ToolException("Missing mandatory parameter '" + S_PARAM + "'. Please, specify the output SAM file.");
        }

        // Check the options ONE_PARAM, TWO_PARAM and U_PARAM, at least one of them has to be specified
        if (!analysisParams.getHisat2Params().getOptions().containsKey(ONE_PARAM)
                && !analysisParams.getHisat2Params().getOptions().containsKey(TWO_PARAM)
                && !analysisParams.getHisat2Params().
                getOptions().containsKey(U_PARAM)) {
            throw new ToolException("Missing mandatory parameters: at least one of '" + ONE_PARAM + "', '" + TWO_PARAM
                    + "' or '" + U_PARAM + "' must be specified.");
        }

        List<String> fileParams = getFileParams(HISAT2_TOOL, analysisParams.getHisat2Params().getOptions());
        List<String> skippedParams = getSkippedParams(HISAT2_TOOL, analysisParams.getHisat2Params().getOptions());
        updatedParams.setOptions(checkParams(analysisParams.getHisat2Params().getOptions(), fileParams, skippedParams, study,
                catalogManager, token));

    }

    private void checkHisat2BuildToolParams() throws ToolException {
        // Check input
        if (CollectionUtils.isEmpty(analysisParams.getHisat2Params().getInput())) {
            throw new ToolException("Missing input parameters: <reference_in> <ht2_index_base>");
        }
        if (analysisParams.getHisat2Params().getInput().size() != 2) {
            throw new ToolException("Invalid number of input parameters for HISAT2 build command. Expected (<reference_in>"
                    + " <ht2_index_base>), but found " + analysisParams.getHisat2Params().getInput().size() + " parameters.");

        }
        List<String> refIn = checkPaths(Arrays.asList(analysisParams.getHisat2Params().getInput().get(0).split(",")), study,
                catalogManager, token);
        updatedParams.getInput().add(StringUtils.join(refIn, ","));
        updatedParams.getInput().add(analysisParams.getHisat2Params().getInput().get(1));


        // Check options
        if (!MapUtils.isEmpty(analysisParams.getHisat2Params().getOptions())) {
            List<String> fileParams = getFileParams(HISAT2_BUILD_TOOL, analysisParams.getHisat2Params().getOptions());
            List<String> skippedParams = getSkippedParams(HISAT2_BUILD_TOOL, analysisParams.getHisat2Params().getOptions());
            updatedParams.setOptions(checkParams(analysisParams.getHisat2Params().getOptions(), fileParams, skippedParams,
                    study, catalogManager, token));
        }
    }

    private List<String> getFileParams(String command, ObjectMap options) {
        List<String> fileParams = new ArrayList<>();
        for (String name : options.keySet()) {
            if (Hisat2Params.isFileParam(command, name)) {
                fileParams.add(name);
            }
        }
        return fileParams;
    }

    private List<String> getSkippedParams(String command, ObjectMap options) {
        List<String> skippedParams = new ArrayList<>();
        for (String name : options.keySet()) {
            if (Hisat2Params.isSkippedParam(command, name)) {
                skippedParams.add(name);
            }
        }
        return skippedParams;
    }
}
