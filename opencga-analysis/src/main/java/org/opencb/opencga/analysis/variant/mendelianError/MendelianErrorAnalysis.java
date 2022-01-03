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

package org.opencb.opencga.analysis.variant.mendelianError;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.MendelianErrorReport;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.MendelianErrorAnalysisExecutor;

import java.io.IOException;

@Tool(id = MendelianErrorAnalysis.ID, resource = Enums.Resource.VARIANT, description = MendelianErrorAnalysis.DESCRIPTION)
public class MendelianErrorAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "mendelian-error";
    public static final String DESCRIPTION = "Run mendelian error analysis to infer uniparental disomy regions.";

    @ToolParams
    protected final MendelianErrorParams params = new MendelianErrorParams();

    public MendelianErrorAnalysis() {
    }

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(getStudy());

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study ID.");
        }

        // Sanity check
        if (StringUtils.isNotEmpty(params.getFamily()) && StringUtils.isNotEmpty(params.getIndividual())
                && StringUtils.isNotEmpty(params.getSample())) {
            throw new ToolException("Incorrect parameters: please, provide only a family ID, a individual ID or a sample ID.");
        }

        // Get family by ID
        Family family;
        if (StringUtils.isNotEmpty(params.getFamily())) {
            // Get family ID by individual ID
            family = IndividualQcUtils.getFamilyById(getStudy(), params.getFamily(), catalogManager, token);
        } else if (StringUtils.isNotEmpty(params.getIndividual())) {
            // Get family ID by individual ID
            family = IndividualQcUtils.getFamilyByIndividualId(getStudy(), params.getIndividual(), catalogManager, token);
        } else if (StringUtils.isNotEmpty(params.getSample())) {
            // Get family ID by sample ID
            family = IndividualQcUtils.getFamilyBySampleId(getStudy(), params.getSample(), catalogManager, token);
        } else {
            throw new ToolException("Missing a family ID, a individual ID or a sample ID.");
        }
        if (family == null) {
            throw new ToolException("Members not found to execute genetic checks analysis.");
        }

        params.setFamily(family.getId());
    }


    @Override
    protected void run() throws ToolException {

        step(ID, () -> {
            MendelianErrorAnalysisExecutor mendelianErrorExecutor = getToolExecutor(MendelianErrorAnalysisExecutor.class);

            mendelianErrorExecutor.setStudyId(getStudy())
                    .setFamilyId(params.getFamily())
                    .execute();

            try {
                // Save inferred sex report
                MendelianErrorReport report = mendelianErrorExecutor.getMendelianErrorReport();
                JacksonUtils.getDefaultObjectMapper().writer().writeValue(getOutDir().resolve(ID + ".report.json").toFile(), report);
            } catch (IOException e) {
                throw new ToolException(e);
            }
        });
    }
}
