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
import org.opencb.opencga.core.models.wrapper.star.StarWrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.OUTPUT_FILE_PREFIX;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.checkParams;
import static org.opencb.opencga.analysis.wrappers.star.StarWrapperAnalysisExecutor.*;

@Tool(id = StarWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = StarWrapperAnalysis.DESCRIPTION)
public class StarWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "star";
    public final static String DESCRIPTION = "STAR (stands for Spliced Transcripts Alignment to a Reference) is a tool to align"
            + " RNA-seq data.";
    public static final String PARAMETER_SET_TO_THE_JOB_DIRECTORY_LOG = "The parameter '{}' is set to the JOB directory instead: {}.";

    @ToolParams
    protected final StarWrapperParams analysisParams = new StarWrapperParams();

    private StarWrapperParams updatedParams;

    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        if (MapUtils.isEmpty(analysisParams.getStarParams())) {
            throw new ToolException("Missing STAR parameters.");
        }

        updatedParams = new StarWrapperParams();

        // Check parameters, and get physical paths from OpenCGA catalog files before passing them to the executor
        updatedParams.setStarParams(checkParams(analysisParams.getStarParams(), study, catalogManager, token));

        // Set output directory to the JOB directory
        if (analysisParams.getStarParams().containsKey(RUN_MODE_PARAM)
                && analysisParams.getStarParams().get(RUN_MODE_PARAM).equals(GENOME_GENERATE_VALUE)) {
            // If the run mode is genomeGenerate, the output directory is the parameter --genomeDir
            logger.warn(PARAMETER_SET_TO_THE_JOB_DIRECTORY_LOG, GENOME_DIR_PARAM, getOutDir().toAbsolutePath());
            updatedParams.getStarParams().put(GENOME_DIR_PARAM, OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath() + "/");
        }
        logger.warn(PARAMETER_SET_TO_THE_JOB_DIRECTORY_LOG, OUT_FILE_NAME_PREFIX_PARAM, getOutDir().toAbsolutePath() + "/");
        updatedParams.getStarParams().put(OUT_FILE_NAME_PREFIX_PARAM, OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath() + "/");
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
                .setStarWrapperParams(updatedParams)
                .execute();
    }
}
