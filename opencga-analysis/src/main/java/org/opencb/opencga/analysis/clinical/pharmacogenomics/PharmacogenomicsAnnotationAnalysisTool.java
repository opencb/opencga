package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.opencga.analysis.variant.operations.OperationTool;
import org.opencb.opencga.core.cellbase.CellBaseValidator;
import org.opencb.opencga.core.models.clinical.PharmacogenomicsAnnotationAnalysisToolParams;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.AlleleTyperResult;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Tool(id = PharmacogenomicsAnnotationAnalysisTool.ID, resource = Enums.Resource.CLINICAL_ANALYSIS, type = Tool.Type.ANALYSIS,
        description = "Pharmacogenomics annotation analysis.", priority = Enums.Priority.HIGH)
public class PharmacogenomicsAnnotationAnalysisTool extends OperationTool {
    public final static String ID = "pharmacogenomics-annotation-analysis";
    public final static String DESCRIPTION = "Annotate the star alleles for the input samples with pharmacogenomics data";

    private CellBaseClient cellBaseClient;

    private List<AlleleTyperResult> alleleTyperResults;

    @ToolParams
    protected final PharmacogenomicsAnnotationAnalysisToolParams analysisParams = new PharmacogenomicsAnnotationAnalysisToolParams();

    private PharmacogenomicsManager pharmacogenomicsManager;

    @Override
    protected void check() throws Exception {
        super.check();

        this.pharmacogenomicsManager = new PharmacogenomicsManager(getCatalogManager());

        // Build the CellBase client from the project's CellBase configuration
        CellBaseValidator cellBaseValidator = pharmacogenomicsManager.buildCellBaseValidator(this.study, getToken());
        if (cellBaseValidator == null) {
            throw new IllegalArgumentException("No CellBase configuration found for study '" + this.study + "'");
        }
        this.cellBaseClient = cellBaseValidator.getCellBaseClient();

        String alleleTyperContent = analysisParams.getAlleleTyperContent();
        if (StringUtils.isEmpty(alleleTyperContent)) {
            throw new IllegalArgumentException("Allele typer content is empty");
        }

        // Deserialize the allele typer content (JSON) into a list of AlleleTyperResult
        ObjectMapper objectMapper = new ObjectMapper();
        this.alleleTyperResults = objectMapper.readValue(alleleTyperContent, new TypeReference<List<AlleleTyperResult>>() {});
    }

    @Override
    protected void run() throws Exception {
        step(this::annotateResults);
    }

    private void annotateResults() throws IOException {
        // Annotate the allele typer results using the pharmacogenomics manager
        pharmacogenomicsManager.annotateResults(alleleTyperResults, cellBaseClient);

        // Save the results to the output directory
        Path resultsPath = getOutDir().resolve(PharmacogenomicsAlleleTyperAnalysisTool.RESULTS_DIR);
        Files.createDirectories(resultsPath);
        pharmacogenomicsManager.storeResultsInPath(alleleTyperResults, resultsPath);
    }

}
