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

package org.opencb.opencga.analysis.clinical.interpreter;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.biodata.tools.clinical.tiering.TieringConfiguration;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.InterpretationAnalysisTool;
import org.opencb.opencga.analysis.clinical.tiering.TieringInterpretationAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.interpreter.InterpreterAnalysisToolParams;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.yaml.snakeyaml.Yaml;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.opencb.opencga.core.tools.ResourceManager.ANALYSIS_DIRNAME;

public class InterpreterAnalysisTool extends InterpretationAnalysisTool {

    public static final String ID = "interpreter";
    public static final String DESCRIPTION = "Run interpreter analysis";

    @ToolParams
    protected final InterpreterAnalysisToolParams analysisToolParams = new InterpreterAnalysisToolParams();

    @Override
    protected InterpretationMethod getInterpretationMethod() {
        return getInterpretationMethod(ID);
    }

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();
        setUpStorageEngineExecutor(study);

        // Check Clinical Analysis exist and permissions
        // 1. Check Clinical Analysis case
        if (StringUtils.isNotEmpty(analysisToolParams.getClinicalAnalysisId())) {
            try {
                this.clinicalAnalysis = catalogManager.getClinicalAnalysisManager()
                        .get(study, analysisToolParams.getClinicalAnalysisId(), QueryOptions.empty(), token)
                        .first();
            } catch (CatalogException e) {
                throw new ToolException(e);
            }
        } else {
            throw new ToolException("Missing clinical analysis ID");
        }

        // 2. Check proband in clinical analysis
        if (clinicalAnalysis.getProband() != null && StringUtils.isNotEmpty(clinicalAnalysis.getProband().getId())) {
            OpenCGAResult<Individual> individualOpenCGAResult = getCatalogManager().getIndividualManager()
                    .get(study, clinicalAnalysis.getProband().getId(), QueryOptions.empty(), token);
            if (individualOpenCGAResult.getNumResults() == 0) {
                throw new ToolException("Proband '" + clinicalAnalysis.getProband().getId() + "' in clinical analysis "
                        + clinicalAnalysis.getId() + " not found.");
            }
        } else {
            throw new ToolException("Missing proband in clinical analysis " + clinicalAnalysis.getId());
        }

        // 3. If family analysis, check family in clinical analysis
        if (clinicalAnalysis.getType().equals(ClinicalAnalysis.Type.FAMILY)) {
            if (clinicalAnalysis.getFamily() != null && StringUtils.isNotEmpty(clinicalAnalysis.getFamily().getId())) {
                OpenCGAResult<Family> familyResult = getCatalogManager().getFamilyManager()
                        .get(study, clinicalAnalysis.getFamily().getId(), QueryOptions.empty(), token);
                if (familyResult.getNumResults() == 0) {
                    throw new ToolException("Family '" + clinicalAnalysis.getFamily().getId() + "' in clinical analysis "
                            + clinicalAnalysis.getId() + " not found.");
                }
            } else {
                throw new ToolException("Missing family in clinical analysis " + clinicalAnalysis.getId());
            }
        }

        // Check the rest of parameters
        // 1. Check primary
        this.primary = analysisToolParams.isPrimary();
        checkPrimaryInterpretation(clinicalAnalysis);

        // Check interpretation method in both primary and secondary interpretations (only one interpretation of each method can exist
        // in the clinical analysis)
        checkInterpretationMethod(getInterpretationMethod(ID).getName(), clinicalAnalysis);

        // Read tiering configuration file from the user parameters or default one if not provided
        Path configPath;
        if (StringUtils.isNotEmpty(analysisToolParams.getConfigFile())) {
            logger.info("Using custom tiering configuration file: {}", analysisToolParams.getConfigFile());
            File opencgaFile = getCatalogManager().getFileManager().get(study, analysisToolParams.getConfigFile(), QueryOptions.empty(), token)
                    .first();
            configPath = Paths.get(opencgaFile.getUri());
        } else {
            logger.info("Using default tiering configuration file");
            configPath = getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve("tiering").resolve("tiering-configuration.yml");
        }
        if (!Files.exists(configPath)) {
            throw new ToolException("Tiering configuration file not found: " + configPath);
        }
        tieringConfiguration = JacksonUtils.getDefaultObjectMapper().convertValue(new Yaml().load(Files.newInputStream(configPath)),
                TieringConfiguration.class);

        // Finally, update tiering configuration with the parameters provided by the user and set default values if needed
        updateTieringConfiguration();
    }

    @Override
    protected void run() throws ToolException {
        step(() -> {
            TieringInterpretationAnalysisExecutor executor = getToolExecutor(TieringInterpretationAnalysisExecutor.class);
            executor.setStudy(study)
                    .setClinicalAnalysis(clinicalAnalysis)
                    .setTieringConfiguration(tieringConfiguration)
                    .execute();

            saveInterpretation(study, clinicalAnalysis, null);
        });
    }

//    public Interpretation run(String clinicalAnalysisId, String probandId, String familyId, List<String> panelIds, String disorderId,
//                              Path configPath) throws Exception {
//        // Set parameters
//        this.clinicalAnalysisId = clinicalAnalysisId;
//        this.probandId = probandId;
//        this.familyId = familyId;
//        this.panelIds = panelIds;
//        this.disorderId = disorderId;
//        this.configPath = configPath;
//
//        // Check parameters before running analysis
//        check();
//
//        // Run
//        run();
//        return null;
//    }
}
