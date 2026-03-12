package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.operations.OperationTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.cellbase.CellBaseValidator;
import org.opencb.opencga.core.models.clinical.PharmacogenomicsAnnotationAnalysisToolParams;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.AlleleTyperResult;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

        // Build the CellBase client only when annotation is requested (default: true)
        boolean annotate = !Boolean.FALSE.equals(analysisParams.getAnnotate());
        if (annotate) {
            CellBaseValidator cellBaseValidator = pharmacogenomicsManager.buildCellBaseValidator(this.study, getToken());
            if (cellBaseValidator == null) {
                logger.warn("No CellBase configuration found for study '{}'. CellBase annotation will be skipped.", this.study);
            } else {
                this.cellBaseClient = cellBaseValidator.getCellBaseClient();
            }
        }

        // Sanity check
        if (StringUtils.isEmpty(analysisParams.getAlleleTyperContent()) && StringUtils.isEmpty(analysisParams.getAlleleTyperFile())) {
            throw new IllegalArgumentException("Allele typer content and file are empty. Please, set one of these parameters.");
        }
        if (StringUtils.isNotEmpty(analysisParams.getAlleleTyperContent())) {
            ObjectMapper objectMapper = new ObjectMapper();
            this.alleleTyperResults = objectMapper.readValue(analysisParams.getAlleleTyperContent(),
                    new TypeReference<List<AlleleTyperResult>>() {});
        } else {
            File opencgaFile = getCatalogManager().getFileManager()
                    .get(study, analysisParams.getAlleleTyperFile(), QueryOptions.empty(), token).first();
            Path filePath = Paths.get(opencgaFile.getUri());
            ObjectMapper objectMapper = new ObjectMapper();
            this.alleleTyperResults = objectMapper.readValue(filePath.toFile(), new TypeReference<List<AlleleTyperResult>>() {});
        }
    }

    @Override
    protected void run() throws Exception {
        step(this::annotateResults);
    }

    private void annotateResults() throws IOException, CatalogException {
        boolean annotate = !Boolean.FALSE.equals(analysisParams.getAnnotate());
        if (annotate) {
            // Annotate with CellBase (star-allele level) + CPIC (diplotype level)
            pharmacogenomicsManager.annotateResults(alleleTyperResults, cellBaseClient);
        }

        // Save the results to the output directory
        Path resultsPath = getOutDir().resolve(PharmacogenomicsAlleleTyperAnalysisTool.RESULTS_DIR);
        Files.createDirectories(resultsPath);
        pharmacogenomicsManager.storeResultsInPath(alleleTyperResults, resultsPath);
        // In addition, the it is store in the sample object in catalog

        pharmacogenomicsManager.storeResultsInCatalog(study, alleleTyperResults, token);
    }

}
