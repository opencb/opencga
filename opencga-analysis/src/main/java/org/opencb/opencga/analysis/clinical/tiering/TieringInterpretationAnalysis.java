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

package org.opencb.opencga.analysis.clinical.tiering;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.ClinicalProperty.Penetrance;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.biodata.tools.clinical.tiering.TieringConfiguration;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.InterpretationAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.tiering.TieringInterpretationAnalysisParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.yaml.snakeyaml.Yaml;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.opencb.opencga.core.tools.ResourceManager.ANALYSIS_DIRNAME;

@Deprecated
@Tool(id = TieringInterpretationAnalysis.ID, resource = Enums.Resource.CLINICAL)
public class TieringInterpretationAnalysis extends InterpretationAnalysis {

    public static final String ID = "interpretation-tiering";
    public static final String DESCRIPTION = "Run tiering interpretation analysis";

    private ClinicalAnalysis clinicalAnalysis;
    private TieringConfiguration tieringConfiguration;

    @ToolParams
    protected final TieringInterpretationAnalysisParams analysisParams = new TieringInterpretationAnalysisParams();

    @Override
    protected InterpretationMethod getInterpretationMethod() {
        return getInterpretationMethod(ID);
    }

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check clinical analysis
        if (StringUtils.isEmpty(analysisParams.getClinicalAnalysisId())) {
            throw new ToolException("Missing clinical analysis ID");
        }

        // Get clinical analysis to check diseases, proband and family
        try {
            clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(study, analysisParams.getClinicalAnalysisId(),
                    QueryOptions.empty(), token).first();
        } catch (
                CatalogException e) {
            throw new ToolException(e);
        }

        // Check disease panels in clinical analysis
        if (CollectionUtils.isEmpty(clinicalAnalysis.getPanels())) {
            throw new ToolException("Missing disease panels in clinical analysis " + clinicalAnalysis.getId());
        }

        // Check proband in clinical analysis
        if (clinicalAnalysis.getProband() == null || StringUtils.isEmpty(clinicalAnalysis.getProband().getId())) {
            throw new ToolException("Missing proband in clinical analysis " + clinicalAnalysis.getId());
        }
        OpenCGAResult<Individual> indvidualResult = getCatalogManager().getIndividualManager().get(study,
                clinicalAnalysis.getProband().getId(), QueryOptions.empty(), token);
        if (indvidualResult.getNumResults() == 0) {
            throw new ToolException("Proband '" + clinicalAnalysis.getProband().getId() + "' in clinical analysis "
                    + clinicalAnalysis.getId() + " not found.");
        }

        // Check family in clinical analysis
        if (clinicalAnalysis.getFamily() == null || StringUtils.isEmpty(clinicalAnalysis.getFamily().getId())) {
            throw new ToolException("Missing family in clinical analysis " + clinicalAnalysis.getId());
        }
        OpenCGAResult<Family> familyResult = getCatalogManager().getFamilyManager().get(study, clinicalAnalysis.getFamily().getId(),
                QueryOptions.empty(), token);
        if (familyResult.getNumResults() == 0) {
            throw new ToolException("Family '" + clinicalAnalysis.getFamily().getId() + "' in clinical analysis "
                    + clinicalAnalysis.getId() + " not found.");
        }

        // Check primary
        this.primary = analysisParams.isPrimary();
        checkPrimaryInterpretation(clinicalAnalysis);

        // Check interpretation method in both primary and secondary interpretations (only one interpretation of each method can exist
        // in the clinical analysis)
        checkInterpretationMethod(getInterpretationMethod(ID).getName(), clinicalAnalysis);

        // Read tiering configuration file from the user parameters or default one if not provided
        Path configPath;
        if (StringUtils.isNotEmpty(analysisParams.getConfigFile())) {
            logger.info("Using custom tiering configuration file: {}", analysisParams.getConfigFile());
            File opencgaFile = getCatalogManager().getFileManager().get(study, analysisParams.getConfigFile(), QueryOptions.empty(), token)
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

    private void updateTieringConfiguration() {
        // Update tiering configuration with the parameters provided by the user
        if (analysisParams.getTieringParams() != null && StringUtils.isNotEmpty(analysisParams.getTieringParams().getPenetrance())) {
            // Check and update penetrance value
            Penetrance penetrance = Penetrance.valueOf(analysisParams.getTieringParams().getPenetrance());
            tieringConfiguration.setPenetrance(penetrance.name());
        }

        // Set update values if not present in the configuration
        if (StringUtils.isEmpty(tieringConfiguration.getPenetrance())) {
            tieringConfiguration.setPenetrance(ClinicalProperty.Penetrance.COMPLETE.name());
        }
    }
}
