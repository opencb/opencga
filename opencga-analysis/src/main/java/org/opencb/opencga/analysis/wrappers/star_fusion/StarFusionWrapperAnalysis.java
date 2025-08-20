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

package org.opencb.opencga.analysis.wrappers.star_fusion;

import org.apache.commons.collections4.MapUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.wrapper.star.StarWrapperParams;
import org.opencb.opencga.core.models.wrapper.star_fusion.StarFusionParams;
import org.opencb.opencga.core.models.wrapper.star_fusion.StarFusionWrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.OUTPUT_FILE_PREFIX;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.checkParams;
import static org.opencb.opencga.core.models.wrapper.star_fusion.StarFusionParams.*;

@Tool(id = StarFusionWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = StarFusionWrapperAnalysis.DESCRIPTION)
public class StarFusionWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "star";
    public final static String DESCRIPTION = "STAR-Fusion is a common tool to predict fusions from the transcriptome which leverages"
            + " discordant and chimeric read alignments from STAR.";
    public static final String PARAMETER_SET_TO_THE_JOB_DIRECTORY_LOG = "The parameter '{}' is set to the JOB directory instead: {}.";

    @ToolParams
    protected final StarFusionWrapperParams analysisParams = new StarFusionWrapperParams();

    private StarFusionParams updatedParams = new StarFusionParams();

    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        if (analysisParams.getStarFusionParams() == null || MapUtils.isEmpty(analysisParams.getStarFusionParams().getOptions())) {
            throw new ToolException("Missing STAR-Fusion parameters.");
        }

        // Check parameters, and get physical paths from OpenCGA catalog files before passing them to the executor
        updatedParams.setOptions(checkParams(analysisParams.getStarFusionParams().getOptions(), FILE_PARAMS,
                SKIPPED_PARAMS, study, catalogManager, token));

        // For all run modes, set the OUT_FILE_NAME_PREFIX_PARAM to the JOB directory
        logger.info(PARAMETER_SET_TO_THE_JOB_DIRECTORY_LOG, OUTPUT_DIR_PARAM, getOutDir().toAbsolutePath());
        updatedParams.getOptions().put(OUTPUT_DIR_PARAM, OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath());
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(ID);
    }

    protected void run() throws ToolException, IOException {
        // Run STAR-Fusion
        step(ID, this::runStarFusion);
    }

    protected void runStarFusion() throws ToolException {
        // Get executor
        StarFusionWrapperAnalysisExecutor executor = getToolExecutor(StarFusionWrapperAnalysisExecutor.class);

        // Set parameters and execute
        executor.setStudy(study)
                .setStarFusionParams(updatedParams)
                .execute();
    }
}
