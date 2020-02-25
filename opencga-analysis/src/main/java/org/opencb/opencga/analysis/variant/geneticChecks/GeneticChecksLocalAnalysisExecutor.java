package org.opencb.opencga.analysis.variant.geneticChecks;

import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.GeneticChecksAnalysisExecutor;

@ToolExecutor(id="opencga-local", tool = GeneticChecksAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class GeneticChecksLocalAnalysisExecutor extends GeneticChecksAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolException {
        VariantStorageManager variantStorageManager = getVariantStorageManager();

        switch (getGeneticCheck()) {
            case SEX:
                AlignmentStorageManager alignmentStorageManager = getAlignmentStorageManager();
                FileManager fileManager = alignmentStorageManager.getCatalogManager().getFileManager();
                KaryotypicSexComputation.compute(getStudy(), getSamples(), fileManager, alignmentStorageManager, getToken());
                break;
            case RELATEDNESS:
                // Run IBD/IBS computation using PLINK in docker
                IBDComputation.compute(getStudy(), getSamples(), getOutDir(), variantStorageManager, getToken());
                break;
            case MENDELIAN_ERRORS:
                MendelianInconsistenciesComputation.compute(getStudy(), getSamples(), getOutDir(), variantStorageManager, getToken());
                break;
            default:
                throw new ToolException("Unknown genetic check: " + getGeneticCheck());
        }
    }
}
