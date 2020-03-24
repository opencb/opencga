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
