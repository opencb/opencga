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

package org.opencb.opencga.analysis.wrappers.liftover;


import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.variant.manager.CatalogUtils;
import org.opencb.opencga.analysis.wrappers.plink.PlinkWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.LiftoverWrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

@Tool(id = LiftoverWrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = LiftoverWrapperAnalysis.DESCRIPTION)
public class LiftoverWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "liftover";
    public static final String DESCRIPTION = "BCFtools liftover plugin maps coordinates from assembly 37 to 38.";

    @ToolParams
    protected final LiftoverWrapperParams analysisParams = new LiftoverWrapperParams();

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(analysisParams.getFile())) {
            throw new ToolException("Liftover 'file' parameter is mandatory.");
        }

        if (StringUtils.isEmpty(analysisParams.getOutdir())) {
            String file = analysisParams.getFile();
            if (file.contains("/")) {
                analysisParams.setOutdir(file.substring(0, file.lastIndexOf('/')));
            } else {
                analysisParams.setOutdir("");
            }
        }
    }

    @Override
    protected void run() throws Exception {
//        setUpStorageEngineExecutor(study);

        step(() -> {
            executorParams.append("study", study);
            executorParams.append("file", analysisParams.getFile());
            executorParams.append("targetAssembly", analysisParams.getTargetAssembly());
            executorParams.append("outdir", analysisParams.getOutdir());

            getToolExecutor(LiftoverWrapperAnalysisExecutor.class)
                    .execute();
        });
    }
}
