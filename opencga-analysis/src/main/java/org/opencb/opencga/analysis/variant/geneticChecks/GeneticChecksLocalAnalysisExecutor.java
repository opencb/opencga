package org.opencb.opencga.analysis.variant.geneticChecks;

import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.models.variant.MendelianErrorsReport;
import org.opencb.opencga.core.models.variant.RelatednessReport;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.GeneticChecksAnalysisExecutor;

import java.util.ArrayList;
import java.util.List;

@ToolExecutor(id="opencga-local", tool = GeneticChecksAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class GeneticChecksLocalAnalysisExecutor extends GeneticChecksAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolException {
        switch (getGeneticCheck()) {

            case INFERRED_SEX: {

                // Get managers
                AlignmentStorageManager alignmentStorageManager = getAlignmentStorageManager();
                CatalogManager catalogManager = alignmentStorageManager.getCatalogManager();
                FileManager fileManager = catalogManager.getFileManager();

                // Get assembly
                String assembly;
                try {
                    assembly = GeneticChecksUtils.getAssembly(getStudyId(), alignmentStorageManager.getCatalogManager(), getToken());
                } catch (CatalogException e) {
                    throw new ToolException(e);
                }

                // Infer the sex for each individual
                List<InferredSexReport> sexReportList = new ArrayList<>();
                for (Individual individual : getIndividuals()) {
                    // Sample is never null, it was checked previously
                    Sample sample = GeneticChecksUtils.getValidSampleByIndividualId(getStudyId(), individual.getId(), catalogManager, getToken());

                    // Compute ratios: X-chrom / autosomic-chroms and Y-chrom / autosomic-chroms
                    double[] ratios = InferredSexComputation.computeRatios(getStudyId(), sample.getId(), assembly, fileManager,
                            alignmentStorageManager, getToken());

                    // Add sex report to the list
                    // TODO infer sex from ratios
                    sexReportList.add(new InferredSexReport(individual.getId(), sample.getId(), individual.getSex().name(),
                            individual.getKaryotypicSex().name(), ratios[0], ratios[1], ""));
                }

                // Set sex report
                getReport().setInferredSexReport(sexReportList);
                break;
            }
            case RELATEDNESS: {

                // Get managers
                VariantStorageManager variantStorageManager = getVariantStorageManager();
                CatalogManager catalogManager = variantStorageManager.getCatalogManager();

                // Get sample IDs from individuals
                List<String> sampleIds = GeneticChecksUtils.getSampleIds(getStudyId(), getIndividuals(), catalogManager, getToken());

                // Run IBD/IBS computation using PLINK in docker
                RelatednessReport relatednessReport = IBDComputation.compute(getStudyId(), sampleIds, getMinorAlleleFreq(), getOutDir(),
                        variantStorageManager, getToken());

                // Set relatedness report
                getReport().setRelatednessReport(relatednessReport);
                break;
            }
            case MENDELIAN_ERRORS: {

                // Get managers
                VariantStorageManager variantStorageManager = getVariantStorageManager();
                CatalogManager catalogManager = variantStorageManager.getCatalogManager();

                // Get sample IDs from individuals
                List<String> sampleIds = GeneticChecksUtils.getSampleIds(getStudyId(), getIndividuals(), catalogManager, getToken());

                // Compute mendelian inconsitencies
                MendelianErrorsReport mendelianErrorsReport = MendelianInconsistenciesComputation.compute(getStudyId(), sampleIds,
                        getOutDir(), variantStorageManager, getToken());

                // Set relatedness report
                getReport().setMendelianErrorsReport(mendelianErrorsReport);
                break;
            }
            default: {
                throw new ToolException("Unknown genetic check: " + getGeneticCheck());
            }
        }
    }
}
