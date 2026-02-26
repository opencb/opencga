package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.variant.operations.OperationTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.clinical.PharmacogenomicsAlleleTyperToolParams;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.AlleleTyperResult;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Tool(id = PharmacogenomicsAlleleTyperAnalysisTool.ID, resource = Enums.Resource.CLINICAL_ANALYSIS, type = Tool.Type.ANALYSIS,
        description = "Pharmacogenomics allele typing analysis.", priority = Enums.Priority.HIGH)
public class PharmacogenomicsAlleleTyperAnalysisTool extends OperationTool {
    public final static String ID = "pharmacogenomics-allele-typer-analysis";
    public final static String DESCRIPTION = "Allele typing and pharmacogenomics annotation";

    public final static String RESULTS_DIR = "results";

    @ToolParams
    protected final PharmacogenomicsAlleleTyperToolParams analysisParams = new PharmacogenomicsAlleleTyperToolParams();

    private PharmacogenomicsManager pharmacogenomicsManager;

    @Override
    protected void check() throws Exception {
        super.check();

        this.pharmacogenomicsManager = new PharmacogenomicsManager(getCatalogManager());

        String genotypingContent = analysisParams.getGenotypingContent();
        if (StringUtils.isEmpty(genotypingContent)) {
            throw new IllegalArgumentException("Genotyping content is empty");
        }

        String translationContent = analysisParams.getTranslationContent();
        if (StringUtils.isEmpty(translationContent)) {
            throw new IllegalArgumentException("Translation content is empty");
        }
    }

    @Override
    protected void run() throws Exception {
        step(this::alleleTyper);
    }

    private void alleleTyper() throws IOException, CatalogException {
        // Perform allele typing using the pharmacogenomics manager
        boolean annotate = Boolean.TRUE.equals(analysisParams.getAnnotate());
        List<AlleleTyperResult> results = pharmacogenomicsManager.alleleTyper(study, analysisParams.getGenotypingContent(),
                analysisParams.getTranslationContent(), annotate, token);

        // Save the results to the output directory
        Path resultsPath = getOutDir().resolve(RESULTS_DIR);
        Files.createDirectories(resultsPath);
        pharmacogenomicsManager.storeResultsInPath(results, resultsPath);
    }
}
