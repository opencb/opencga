package org.opencb.opencga.analysis.variant.geneticChecks;

import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.variant.MendelianErrorsReport;
import org.opencb.opencga.core.models.variant.RelatednessReport;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.GeneticChecksAnalysisExecutor;

import java.util.List;

@ToolExecutor(id="opencga-local", tool = GeneticChecksAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class GeneticChecksLocalAnalysisExecutor extends GeneticChecksAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolException {
        switch (getGeneticCheck()) {
            case INFERRED_SEX: {
                // Compute karyotypic sex
                AlignmentStorageManager alignmentStorageManager = getAlignmentStorageManager();
                FileManager fileManager = alignmentStorageManager.getCatalogManager().getFileManager();
//                List<double[]> ratios = InferredSexComputation.computeRatios(getStudy(), getSamples(), fileManager,
//                        alignmentStorageManager, getToken());

                // Set sex report
                //getOutput().setInferredSexReport(sexReportList);
                break;
            }
            case RELATEDNESS: {
                // Check relatednessMethod
                // Run IBD/IBS computation using PLINK in docker
                RelatednessReport relatednessReport = IBDComputation.compute(getStudy(), getSamples(), getMinorAlleleFreq(),
                        getOutDir(), getVariantStorageManager(), getToken());

                // Set relatedness report
                getOutput().setRelatednessReport(relatednessReport);
                break;
            }
            case MENDELIAN_ERRORS: {
                // Compute mendelian inconsitencies
                MendelianErrorsReport mendelianErrorsReport = MendelianInconsistenciesComputation.compute(getStudy(), getFamily(),
                        getSamples(), getOutDir(), getVariantStorageManager(), getToken());

                // Set relatedness report
                getOutput().setMendelianErrorsReport(mendelianErrorsReport);
                break;
            }
            default: {
                throw new ToolException("Unknown genetic check: " + getGeneticCheck());
            }
        }
    }
}
