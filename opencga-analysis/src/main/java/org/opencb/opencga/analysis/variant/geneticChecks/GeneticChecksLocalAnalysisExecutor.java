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
        switch (getGeneticCheck()) {
            case SEX: {
                // Compute karyotypic sex
                System.out.println("Not yet implemented");
//                AlignmentStorageManager alignmentStorageManager = getAlignmentStorageManager();
//                FileManager fileManager = alignmentStorageManager.getCatalogManager().getFileManager();
//                KaryotypicSexComputation.compute(getStudy(), getSamples(), fileManager, alignmentStorageManager, getToken());
                break;
            }
            case RELATEDNESS: {
                // Check relatednessMethod
                // Run IBD/IBS computation using PLINK in docker
                IBDComputation.compute(getStudy(), getSamples(), getPopulation(), getOutDir(), getVariantStorageManager(), getToken());
                break;
            }
            case MENDELIAN_ERRORS: {
                // Compute mendelian inconsitencies
                MendelianInconsistenciesComputation.compute(getStudy(), getSamples(), getPopulation(), getOutDir(), getVariantStorageManager(), getToken());
                break;
            }
            default: {
                throw new ToolException("Unknown genetic check: " + getGeneticCheck());
            }
        }
    }
}
