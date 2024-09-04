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

package org.opencb.opencga.analysis.family.qc;

import org.opencb.opencga.analysis.variant.qc.VariantQcAnalysis;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.FamilyQcAnalysisParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

@Tool(id = FamilyVariantQcAnalysis.ID, resource = Enums.Resource.SAMPLE, description = FamilyVariantQcAnalysis.DESCRIPTION)
public class FamilyVariantQcAnalysis extends VariantQcAnalysis {

    public static final String ID = "family-variant-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given family. It computes the relatedness scores among the"
            + " family members";

    @ToolParams
    protected final FamilyQcAnalysisParams analysisParams = new FamilyQcAnalysisParams();

    @Override
    protected void check() throws Exception {
        super.check();
    }

    @Override
    protected void run() throws ToolException {
    }

    public static void checkParameters(FamilyQcAnalysisParams params, String study, CatalogManager catalogManager, String token) {
        checkParameters(params, study, catalogManager, token);
    }
}
