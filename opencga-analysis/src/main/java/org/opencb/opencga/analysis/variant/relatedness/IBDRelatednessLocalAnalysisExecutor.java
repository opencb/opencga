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

import org.opencb.biodata.models.clinical.qc.RelatednessReport;
import org.opencb.opencga.analysis.ConfigurationUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.family.qc.IBDComputation;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.IBDRelatednessAnalysisExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.opencb.opencga.analysis.variant.relatedness.RelatednessAnalysis.VARIANTS_FRQ;
import static org.opencb.opencga.analysis.variant.relatedness.RelatednessAnalysis.VARIANTS_PRUNE_IN;

@ToolExecutor(id="opencga-local", tool = RelatednessAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class IBDRelatednessLocalAnalysisExecutor extends IBDRelatednessAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolException, ResourceException {
        // Get managers
        VariantStorageManager variantStorageManager = getVariantStorageManager();

        // Sanity check to compute
        String opencgaHome = getExecutorParams().getString("opencgaHome");
        if (!Paths.get(opencgaHome).toFile().exists()) {

        }

        // Run IBD/IBS computation using PLINK in docker
        ResourceManager resourceManager = new ResourceManager(Paths.get(getExecutorParams().getString("opencgaHome")));
        String resourceName = ConfigurationUtils.getToolResource(RelatednessAnalysis.ID, null, VARIANTS_PRUNE_IN, getConfiguration());
        Path pruneInPath = resourceManager.checkResourcePath(resourceName);
        resourceName = ConfigurationUtils.getToolResource(RelatednessAnalysis.ID, null, VARIANTS_FRQ, getConfiguration());
        Path freqPath = resourceManager.checkResourcePath(resourceName);

        RelatednessReport report = IBDComputation.compute(getStudyId(), getFamily(), getSampleIds(), getMinorAlleleFreq(), getThresholds(),
                pruneInPath, freqPath, getOutDir(), variantStorageManager, getToken());

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
