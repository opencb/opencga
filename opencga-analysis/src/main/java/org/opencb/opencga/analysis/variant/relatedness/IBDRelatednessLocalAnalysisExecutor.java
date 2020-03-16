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
