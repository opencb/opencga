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

package org.opencb.opencga.analysis.individual.qc;

import org.opencb.opencga.analysis.variant.qc.VariantQcAnalysis;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.IndividualQcAnalysisParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

@Tool(id = IndividualVariantQcAnalysis.ID, resource = Enums.Resource.SAMPLE, description = IndividualVariantQcAnalysis.DESCRIPTION)
public class IndividualVariantQcAnalysis extends VariantQcAnalysis {

    public static final String ID = "individual-variant-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given individual. It includes inferred sex and " +
            " mendelian errors (UDP)";

    @ToolParams
    protected final IndividualQcAnalysisParams analysisParams = new IndividualQcAnalysisParams();

    @Override
    protected void check() throws Exception {
        super.check();
    }

    @Override
    protected void run() throws ToolException {
        // Export variants (VCF format)

        // Export catalog info (JSON format)

        // Execute Python script:
        //    variant_qc.main.py --vcf-file xxx --info-json xxx --bam-file xxx --qc-type xxx --config xxx --output-dir xxx

        // Parse results
    }
}
