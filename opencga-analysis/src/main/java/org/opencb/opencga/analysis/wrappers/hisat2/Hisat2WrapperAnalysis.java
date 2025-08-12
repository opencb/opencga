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
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.wrapper.Hisat2WrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.*;
import static org.opencb.opencga.analysis.wrappers.hisat2.Hisat2WrapperAnalysisExecutor.*;

@Tool(id = Hisat2WrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = Hisat2WrapperAnalysis.DESCRIPTION)
public class Hisat2WrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "hisat2";
    public final static String DESCRIPTION = "HISAT2 is a fast and sensitive alignment program for mapping next-generation sequencing"
            + " reads (whole-genome, transcriptome, and exome sequencing data) against the general human population.";

    private final static String SAM_FILENAME_DEFAULT = "output.sam";

    @ToolParams
    protected final Hisat2WrapperParams analysisParams = new Hisat2WrapperParams();

    private Hisat2WrapperParams updatedParams;

    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        updatedParams = new Hisat2WrapperParams();

        // Check parameters, and get physical paths from OpenCGA catalog files before passing them to the executor

        // Check command
        if (StringUtils.isEmpty(analysisParams.getHisat2Params().getCommand())) {
            throw new ToolException("Missing command. Please, specify the HISAT2 command to run (e.g., 'hisat2', 'hisat2-build',...).");
        }
        String command = analysisParams.getHisat2Params().getCommand().toLowerCase();
        updatedParams.getHisat2Params().setCommand(command);

        // Check input
        if (CollectionUtils.isNotEmpty(analysisParams.getHisat2Params().getInput())) {
            if (analysisParams.getHisat2Params().getInput().size() == 2
                    && (command.equals(HISAT2_BUILD_TOOL) || command.equals(HISAT2_BUILD_L_TOOL) || command.equals(HISAT2_BUILD_S_TOOL))) {
                List<String> input = checkPaths((List<String>) analysisParams.getHisat2Params().getInput().get(0), study, catalogManager,
                        token);
                updatedParams.getHisat2Params().getInput().add(input);
                // Set output directory to the JOB directory
                String basename = Paths.get(analysisParams.getHisat2Params().getInput().get(1).toString()).getFileName().toString();
                logger.warn("The parameter positional '<ht2_index_base>' will be overwritten by the JOB directory instead.");
                updatedParams.getHisat2Params().getInput().add(OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath() + "/" + basename);
            } else {
                throw new ToolException("Not yet implemented: HISAT2 command '" + command + "' with input size "
                        + analysisParams.getHisat2Params().getInput().size() + ". Expected 2 for 'hisat2-build' commands.");
            }
        }

        // Check HISAT2 parameters
        if (MapUtils.isNotEmpty(analysisParams.getHisat2Params().getParams())) {
            String samFilename = analysisParams.getHisat2Params().getParams().getString(S_PARAM);
            if (StringUtils.isEmpty(samFilename)) {
                samFilename = SAM_FILENAME_DEFAULT;
            } else {
                if (samFilename.startsWith(FILE_PREFIX)) {
                    samFilename = Paths.get(samFilename.substring(FILE_PREFIX.length())).getFileName().toString();
                } else {
                    samFilename = Paths.get(samFilename).getFileName().toString();
                }
            }
            if (command.equals(HISAT2_TOOL)) {
                analysisParams.getHisat2Params().getParams().remove(S_PARAM);
            }

            updatedParams.getHisat2Params().setParams(checkParams(analysisParams.getHisat2Params().getParams(), study, catalogManager,
                    token));

            if (command.equals(HISAT2_TOOL)) {
                // Set output directory to the JOB directory
                logger.warn("The parameter '{}' will be overwritten by the JOB directory instead (filename = {}).", S_PARAM, samFilename);
                updatedParams.getHisat2Params().getParams().put(S_PARAM, OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath() + "/"
                        + samFilename);
            }
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
}
