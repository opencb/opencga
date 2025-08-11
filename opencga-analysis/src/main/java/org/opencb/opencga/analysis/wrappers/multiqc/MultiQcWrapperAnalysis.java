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

package org.opencb.opencga.analysis.wrappers.multiqc;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.wrapper.MultiQcWrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.*;
import static org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysisExecutor.OUTDIR_PARAM;

@Tool(id = MultiQcWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = MultiQcWrapperAnalysis.DESCRIPTION)
public class MultiQcWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "multiqc";
    public final static String DESCRIPTION = "MultiQC is an essential reporting tool in bioinformatics that aggregates results and"
            + " statistics from a wide range of bioinformatics tools into a single, interactive HTML report.";

    @ToolParams
    protected final MultiQcWrapperParams analysisParams = new MultiQcWrapperParams();

    private MultiQcWrapperParams updatedParams;

    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

         updatedParams = new MultiQcWrapperParams();

        // Check parameters, and get physical paths from OpenCGA catalog files before passing them to the executor

        // Check input
        if (CollectionUtils.isEmpty(analysisParams.getMultiQcParams().getInput())) {
            throw new ToolException("Missing input paths. At least one input path must be provided.");
        }
        List<String> input = checkPaths(analysisParams.getMultiQcParams().getInput(), study, catalogManager, token);
        updatedParams.getMultiQcParams().setInput(input);

        // Check MultiQC parameters
        if (MapUtils.isNotEmpty(analysisParams.getMultiQcParams().getParams())) {
            updatedParams.getMultiQcParams().setParams(checkParams(analysisParams.getMultiQcParams().getParams(), study, catalogManager,
                    token));
        }

        // Set output directory to the JOB directory
        logger.warn("The parameter '{}' will be overwritten by the JOB directory instead.", OUTDIR_PARAM);
        updatedParams.getMultiQcParams().getParams().put(OUTDIR_PARAM, OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath());
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(ID);
    }

    protected void run() throws ToolException, IOException {
        // Run MultiQC
        step(ID, this::runMultiQc);
    }

    protected void runMultiQc() throws ToolException {
        // Get executor
        MultiQcWrapperAnalysisExecutor executor = getToolExecutor(MultiQcWrapperAnalysisExecutor.class);

        // Set parameters and execute
        executor.setStudy(study)
                .setMultiQcWrapperParams(updatedParams)
                .execute();
    }
}
