package org.opencb.opencga.analysis.variant.inferredSex;

import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.variant.geneticChecks.GeneticChecksUtils;
import org.opencb.opencga.analysis.variant.geneticChecks.InferredSexComputation;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.InferredSexAnalysisExecutor;

import java.io.IOException;
import java.util.List;

@ToolExecutor(id="opencga-local", tool = InferredSexAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class InferredSexLocalAnalysisExecutor extends InferredSexAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolException {
        AlignmentStorageManager alignmentStorageManager = getAlignmentStorageManager();
        FileManager fileManager = alignmentStorageManager.getCatalogManager().getFileManager();
        String assembly;
        try {
            assembly = GeneticChecksUtils.getAssembly(getStudy(), alignmentStorageManager.getCatalogManager(), getToken());
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Infer karyotypic sex
        double[] ratios = InferredSexComputation.computeRatios(getStudy(), getSample(), assembly, fileManager,
                alignmentStorageManager, getToken());

        // Set inferred sex report
        setInferredSexReport(new InferredSexReport("", "", ratios[0], ratios[1], ""));
    }
}
