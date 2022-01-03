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

package org.opencb.opencga.analysis.variant.inferredSex;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.InferredSexReport;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.InferredSexAnalysisExecutor;

import java.io.IOException;

@Tool(id = InferredSexAnalysis.ID, resource = Enums.Resource.VARIANT, description = InferredSexAnalysis.DESCRIPTION)
public class InferredSexAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "inferred-sex";
    public static final String DESCRIPTION = "Infer sex from chromosome mean coverages.";

    @ToolParams
    protected final InferredSexParams inferredSexParams = new InferredSexParams();

    public InferredSexAnalysis() {
    }

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(getStudy());

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study.");
        }

        // Check individual and sample
        if (StringUtils.isEmpty(inferredSexParams.getIndividualId())) {
            throw new ToolException("Missing individual ID.");
        }
    }

    @Override
    protected void run() throws ToolException {

        step("inferred-sex", () -> {
            InferredSexAnalysisExecutor inferredSexExecutor = getToolExecutor(InferredSexAnalysisExecutor.class);

            inferredSexExecutor.setStudyId(getStudy())
                    .setIndividualId(inferredSexParams.getIndividualId())
                    .execute();

            // Get inferred sex report
            InferredSexReport report = inferredSexExecutor.getInferredSexReport();

            try {
                // Save inferred sex report
                JacksonUtils.getDefaultObjectMapper().writer().writeValue(getOutDir().resolve(ID + ".report.json").toFile(), report);
            } catch (IOException e) {
                throw new ToolException(e);
            }
        });
    }
}
