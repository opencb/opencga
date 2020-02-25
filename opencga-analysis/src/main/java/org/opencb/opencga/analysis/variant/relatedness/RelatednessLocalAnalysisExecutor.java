package org.opencb.opencga.analysis.variant.relatedness;

import org.opencb.opencga.analysis.variant.geneticChecks.IBDComputation;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.RelatednessAnalysisExecutor;

import java.io.File;

@ToolExecutor(id="opencga-local", tool = RelatednessAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class RelatednessLocalAnalysisExecutor extends RelatednessAnalysisExecutor implements VariantStorageToolExecutor {

    @Override
    public void run() throws ToolException {
        VariantStorageManager storageManager = getVariantStorageManager();

        // Run IBD/IBS computation using PLINK in docker
        IBDComputation.compute(getStudy(), getSamples(), getOutDir(), storageManager, getToken());
    }
}
