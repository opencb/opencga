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

package org.opencb.opencga.analysis.wrappers.star;

import org.apache.commons.collections4.MapUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.wrapper.star.StarParams;
import org.opencb.opencga.core.models.wrapper.star.StarWrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.OUTPUT_FILE_PREFIX;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.checkParams;
import static org.opencb.opencga.core.models.wrapper.star.StarParams.*;

@Tool(id = StarWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = StarWrapperAnalysis.DESCRIPTION)
public class StarWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "star";
    public final static String DESCRIPTION = "STAR (stands for Spliced Transcripts Alignment to a Reference) is a tool to align"
            + " RNA-seq data.";
    public static final String PARAMETER_SET_TO_THE_JOB_DIRECTORY_LOG = "The parameter '{}' is set to the JOB directory instead: {}.";

    @ToolParams
    protected final StarWrapperParams analysisParams = new StarWrapperParams();

    private StarParams updatedParams = new StarParams();;

    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        if (analysisParams.getStarParams() == null || MapUtils.isEmpty(analysisParams.getStarParams().getOptions())) {
            throw new ToolException("Missing STAR parameters.");
        }

        // Check parameters, and get physical paths from OpenCGA catalog files before passing them to the executor
        updatedParams.setOptions(checkParams(analysisParams.getStarParams().getOptions(), study, catalogManager, token));

        // Set output directory to the JOB directory
        String runMode = analysisParams.getStarParams().getOptions().getString(RUN_MODE_PARAM, ALIGN_READS_VALUE);
        switch (runMode) {
            case ALIGN_READS_VALUE: {
                // Remove the OUT_FILE_NAME_PREFIX_PARAM if it exists, since it will be set to the JOB directory
                List<String> ignoredParams = new ArrayList<>(SKIP_PARAMS);
                ignoredParams.add(OUT_FILE_NAME_PREFIX_PARAM);
                updatedParams.setOptions(checkParams(analysisParams.getStarParams().getOptions(), StarParams.FILE_PARAMS, ignoredParams,
                        study, catalogManager, token));
                break;
            }

            case GENOME_GENERATE_VALUE: {
                // Remove the GENOME_DIR_PARAM if it exists, since it will be set to the JOB directory
                List<String> ignoredParams = new ArrayList<>(SKIP_PARAMS);
                ignoredParams.add(GENOME_DIR_PARAM);
                updatedParams.setOptions(checkParams(analysisParams.getStarParams().getOptions(), StarParams.FILE_PARAMS, ignoredParams,
                        study, catalogManager, token));

                // Set the GENOME_DIR_PARAM to the JOB directory
                logger.info(PARAMETER_SET_TO_THE_JOB_DIRECTORY_LOG, GENOME_DIR_PARAM, getOutDir().toAbsolutePath());
                updatedParams.getOptions().put(GENOME_DIR_PARAM, OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath() + "/");
                break;
            }

            case INPUT_ALIGNMENTS_FROM_BAM_VALUE:
            case LIFT_OVER_VALUE:
            case SOLO_CELL_FILTERING_VALUE: {
                throw new ToolException("Run mode '" + runMode + "' is not supported yet.");
            }

            default:
                throw new ToolException("Unknown run mode: " + runMode + ". Supported run modes are: "
                        + Arrays.asList(ALIGN_READS_VALUE, GENOME_GENERATE_VALUE));
        }

        // For all run modes, set the OUT_FILE_NAME_PREFIX_PARAM to the JOB directory
        logger.info(PARAMETER_SET_TO_THE_JOB_DIRECTORY_LOG, OUT_FILE_NAME_PREFIX_PARAM, getOutDir().toAbsolutePath());
        updatedParams.getOptions().put(OUT_FILE_NAME_PREFIX_PARAM, OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath() + "/");
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(ID);
    }

    protected void run() throws ToolException, IOException {
        // Run STAR
        step(ID, this::runStar);
    }

    protected void runStar() throws ToolException {
        // Get executor
        StarWrapperAnalysisExecutor executor = getToolExecutor(StarWrapperAnalysisExecutor.class);

        // Set parameters and execute
        executor.setStudy(study)
                .setStarParams(updatedParams)
                .execute();
    }
}
