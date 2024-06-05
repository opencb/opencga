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
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ExomiserWrapperParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;


@Tool(id = ExomiserWrapperAnalysis.ID, resource = Enums.Resource.CLINICAL_ANALYSIS,
        description = ExomiserWrapperAnalysis.DESCRIPTION)
public class ExomiserWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "exomiser";
    public final static String DESCRIPTION = "The Exomiser is a Java program that finds potential disease-causing variants"
            + " from whole-exome or whole-genome sequencing data.";

    // It must match the tool prefix in the tool keys for exomiser in the configuration file
    public final static String EXOMISER_PREFIX = "exomiser-";

    // It must match the resources key in the exomiser/tool section in the configuration file
    public final static String HG38_RESOURCE_KEY = "HG38";
    public final static String PHENOTYPE_RESOURCE_KEY = "PHENOTYPE";

    @ToolParams
    protected final ExomiserWrapperParams analysisParams = new ExomiserWrapperParams();

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        // Check exomiser version
        if (StringUtils.isEmpty(analysisParams.getExomiserVersion())) {
            // Missing exomiser version use the default one
            String exomiserVersion = ConfigurationUtils.getToolDefaultVersion(ExomiserWrapperAnalysis.ID, configuration);
            logger.warn("Missing exomiser version, using the default {}", exomiserVersion);
            analysisParams.setExomiserVersion(exomiserVersion);
        }
    }

    @Override
    protected void run() throws Exception {
        setUpStorageEngineExecutor(study);

        step(() -> {
            getToolExecutor(ExomiserWrapperAnalysisExecutor.class)
                    .setStudyId(study)
                    .setSampleId(analysisParams.getSample())
                    .setExomiserVersion(analysisParams.getExomiserVersion())
                    .execute();
        });
    }
}
