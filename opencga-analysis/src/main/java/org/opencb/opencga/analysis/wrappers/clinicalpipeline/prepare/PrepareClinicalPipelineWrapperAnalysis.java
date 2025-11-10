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

package org.opencb.opencga.analysis.wrappers.clinicalpipeline.prepare;


import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.pipeline.prepare.PrepareClinicalPipelineParams;
import org.opencb.opencga.core.models.clinical.pipeline.prepare.PrepareClinicalPipelineWrapperParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils.PIPELINE_ANALYSIS_DIRNAME;
import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils.isURL;
import static org.opencb.opencga.catalog.utils.ResourceManager.ANALYSIS_DIRNAME;

@Tool(id = PrepareClinicalPipelineWrapperAnalysis.ID, resource = Enums.Resource.VARIANT,
        description = PrepareClinicalPipelineWrapperAnalysis.DESCRIPTION)
public class PrepareClinicalPipelineWrapperAnalysis extends OpenCgaTool {

    public static final String ID = "ngs-pipeline-prepare";
    public static final String DESCRIPTION = "Prepare the clinical pipeline.";

    PrepareClinicalPipelineParams updatedParams = new PrepareClinicalPipelineParams();

    @ToolParams
    protected final PrepareClinicalPipelineWrapperParams analysisParams = new PrepareClinicalPipelineWrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check reference genome
        String referenceGenome = analysisParams.getPipelineParams().getReferenceGenome();
        if (StringUtils.isEmpty(referenceGenome)) {
            throw new ToolException("Missing reference genome to prepare the clinical pipeline.");
        }
        if (!isURL(referenceGenome)) {
            logger.info("Checking reference genome file {}", referenceGenome);
            File opencgaFile = getCatalogManager().getFileManager().get(study, referenceGenome, QueryOptions.empty(), token).first();
            referenceGenome = Paths.get(opencgaFile.getUri()).toAbsolutePath().toString();
        }
        updatedParams.setReferenceGenome(referenceGenome);

        // Add the aligner indexes if provided
        if (CollectionUtils.isNotEmpty(analysisParams.getPipelineParams().getIndexes())) {
            updatedParams.setIndexes(analysisParams.getPipelineParams().getIndexes());
        }
    }


    @Override
    protected List<String> getSteps() {
        return Collections.singletonList(ID);
    }

    protected void run() throws ToolException, IOException {
        step(ID, this::runPipelinePrepareExecutor);
    }

    protected void runPipelinePrepareExecutor() throws  ToolException {
        // Get executor
        PrepareClinicalPipelineWrapperAnalysisExecutor executor = getToolExecutor(PrepareClinicalPipelineWrapperAnalysisExecutor.class);

        // Set parameters and execute (depending on the updated params, it will prepare or execute the pipeline)
        executor.setStudy(study)
                .setScriptPath(getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve(PIPELINE_ANALYSIS_DIRNAME))
                .setPrepareParams(updatedParams)
                .execute();
    }
}
