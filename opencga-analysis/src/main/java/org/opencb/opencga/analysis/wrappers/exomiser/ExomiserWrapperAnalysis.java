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

package org.opencb.opencga.analysis.wrappers.exomiser;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.ConfigurationUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ExomiserWrapperParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;


@Tool(id = ExomiserWrapperAnalysis.ID, resource = Enums.Resource.CLINICAL_ANALYSIS,
        description = ExomiserWrapperAnalysis.DESCRIPTION)
public class ExomiserWrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "exomiser";
    public static final String DESCRIPTION = "The Exomiser is a Java program that finds potential disease-causing variants"
            + " from whole-exome or whole-genome sequencing data.";

    // It must match the tool prefix in the tool keys for exomiser in the configuration file
    public static final String EXOMISER_PREFIX = "exomiser-";

    private static final String PREPARE_RESOURCES_STEP = "prepare-resources";
    private static final String EXPORT_VARIANTS_STEP = "export-variants";

    @ToolParams
    protected final ExomiserWrapperParams analysisParams = new ExomiserWrapperParams();

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(study)) {
            throw new ToolException("Missing study");
        }

        setUpStorageEngineExecutor(study);

        // Check exomiser version
        if (StringUtils.isEmpty(analysisParams.getExomiserVersion())) {
            // Missing exomiser version use the default one
            String exomiserVersion = ConfigurationUtils.getToolDefaultVersion(ExomiserWrapperAnalysis.ID, configuration);
            logger.warn("Missing exomiser version, using the default {}", exomiserVersion);
            analysisParams.setExomiserVersion(exomiserVersion);
        }

        ExomiserAnalysisUtils.checkResources(analysisParams.getExomiserVersion(), study, catalogManager, token, getOpencgaHome());
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(PREPARE_RESOURCES_STEP, EXPORT_VARIANTS_STEP, ExomiserWrapperAnalysis.ID);
    }

    @Override
    protected void run() throws Exception {
        // Main steps
        step(PREPARE_RESOURCES_STEP, this::prepareResources);
        step(EXPORT_VARIANTS_STEP, this::exportVariants);
        step(ID, this::runExomiser);
    }

    private void prepareResources() throws ToolException {
        ExomiserAnalysisUtils.prepareResources(analysisParams.getSample(), study, analysisParams.getClinicalAnalysisType(),
                analysisParams.getExomiserVersion(), catalogManager, token, getOutDir(), getOpencgaHome());
    }

    private void exportVariants() throws ToolException {
        ExomiserAnalysisUtils.exportVariants(analysisParams.getSample(), study, analysisParams.getClinicalAnalysisType(), getOutDir(),
                variantStorageManager, token);
    }

    private void runExomiser() throws ToolException, CatalogException {
        ExomiserWrapperAnalysisExecutor executor = getToolExecutor(ExomiserWrapperAnalysisExecutor.class)
                .setExomiserVersion(analysisParams.getExomiserVersion())
                .setAssembly(ExomiserAnalysisUtils.checkAssembly(study, catalogManager, token))
                .setExomiserDataPath(Paths.get(getOpencgaHome().toAbsolutePath().toAbsolutePath().toString(),
                                ResourceManager.ANALYSIS_FOLDER_NAME, ResourceManager.RESOURCES_FOLDER_NAME, ExomiserWrapperAnalysis.ID))
                .setSampleFile(ExomiserAnalysisUtils.getSamplePath(analysisParams.getSample(), getOutDir()))
                .setVcfFile(ExomiserAnalysisUtils.getVcfPath(analysisParams.getSample(), getOutDir()));

        for (File file : getOutDir().toFile().listFiles()) {
            if (file.getName().endsWith(".ped")) {
                executor.setPedigreeFile(file.toPath());
                break;
            }
        }

        executor.execute();
    }
}
