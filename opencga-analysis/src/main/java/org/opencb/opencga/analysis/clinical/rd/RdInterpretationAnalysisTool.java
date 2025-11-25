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

package org.opencb.opencga.analysis.clinical.rd;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.InterpretationAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.interpretation.RdInterpretationAnalysisToolParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.opencb.opencga.catalog.utils.ParamUtils.SaveInterpretationAs.PRIMARY;
import static org.opencb.opencga.catalog.utils.ParamUtils.SaveInterpretationAs.SECONDARY;
import static org.opencb.opencga.core.tools.ResourceManager.ANALYSIS_DIRNAME;

@Tool(id = RdInterpretationAnalysisTool.ID, resource = Enums.Resource.CLINICAL)
public class RdInterpretationAnalysisTool extends InterpretationAnalysis {

    public static final String ID = "interpretation-rd";
    public static final String DESCRIPTION = "Run interpretation analysis for rare diseases";

    public static final String RD_DIR = "rd";
    public static final String RD_INTERPRETATION_CONFIGURATION_FILE = "rd-interpretation-configuration.yml";

    private Path configPath;

    @ToolParams
    protected final RdInterpretationAnalysisToolParams analysisParams = new RdInterpretationAnalysisToolParams();

    @Override
    protected InterpretationMethod getInterpretationMethod() {
        return getInterpretationMethod(ID);
    }

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Read clinical interpretation configuration file from the user parameters or default one if not provided
        if (StringUtils.isNotEmpty(analysisParams.getConfigFile())) {
            logger.info("User provides the RD interpretation configuration file: {}", analysisParams.getConfigFile());
            File opencgaFile = getCatalogManager().getFileManager().get(study, analysisParams.getConfigFile(), QueryOptions.empty(), token)
                    .first();
            configPath = Paths.get(opencgaFile.getUri());
        } else {
            logger.info("Using default configuration file for the RD interpretation analysis");
            configPath = getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve(RD_DIR).resolve(RD_INTERPRETATION_CONFIGURATION_FILE);
        }
        if (!Files.exists(configPath)) {
            throw new ToolException("RD interpretation configuration file not found: " + configPath);
        }
    }

    @Override
    protected void run() throws ToolException {
        step(this::runRdInterpretationAnalysis);
    }

    private void runRdInterpretationAnalysis() throws ToolException {
        RdInterpretationAnalysis rdInterpretationAnalysis = new RdInterpretationAnalysis(study, getCatalogManager(),
                getVariantStorageManager(), token);

        Interpretation interpretation;
        try {
            interpretation = rdInterpretationAnalysis.run(analysisParams.getClinicalAnalysisId(), configPath);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Set name and description
        interpretation.setName(analysisParams.getName())
                .setDescription(analysisParams.getDescription());

        // Add interpretation in the clinical analysis and then save in catalog
        try {
            catalogManager.getInterpretationManager().create(study, analysisParams.getClinicalAnalysisId(),
                    new org.opencb.opencga.core.models.clinical.Interpretation(interpretation),
                    analysisParams.isPrimary() ? PRIMARY : SECONDARY, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error saving interpretation into OpenCGA catalog", e);
        }
    }
}
