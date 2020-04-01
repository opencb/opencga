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

import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.variant.geneticChecks.GeneticChecksUtils;
import org.opencb.opencga.analysis.variant.geneticChecks.InferredSexComputation;
import org.opencb.opencga.analysis.variant.geneticChecks.MendelianInconsistenciesComputation;
import org.opencb.opencga.analysis.variant.inferredSex.InferredSexAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.models.variant.MendelianErrorReport;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.InferredSexAnalysisExecutor;
import org.opencb.opencga.core.tools.variant.MendelianErrorAnalysisExecutor;

@ToolExecutor(id="opencga-local", tool = MendelianErrorAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class MendelianErrorLocalAnalysisExecutor extends MendelianErrorAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolException {
        // Compute
        MendelianErrorReport report = MendelianInconsistenciesComputation.compute(getStudyId(), getFamilyId(), getVariantStorageManager(),
                getToken());

        setMendelianErrorReport(report);
    }
}
