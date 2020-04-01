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

package org.opencb.opencga.analysis.variant.relatedness;

import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.variant.geneticChecks.GeneticChecksUtils;
import org.opencb.opencga.analysis.variant.geneticChecks.IBDComputation;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.variant.RelatednessReport;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.IBDRelatednessAnalysisExecutor;

import java.io.IOException;
import java.util.List;

@ToolExecutor(id="opencga-local", tool = RelatednessAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class IBDRelatednessLocalAnalysisExecutor extends IBDRelatednessAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolException {
        // Get managers
        VariantStorageManager variantStorageManager = getVariantStorageManager();
        CatalogManager catalogManager = variantStorageManager.getCatalogManager();

        // Run IBD/IBS computation using PLINK in docker
        RelatednessReport report = IBDComputation.compute(getStudyId(), getSampleIds(), getMinorAlleleFreq(), getOutDir(),
                variantStorageManager, getToken());

        // Sanity check
        if (report == null) {
            throw new ToolException("Something wrong when executing relatedness analysis");
        }

        // Save results
        try {
            JacksonUtils.getDefaultObjectMapper().writer().writeValue(getOutDir().resolve(RelatednessAnalysis.ID + ".report.json").toFile(),
                    report);
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }
}
