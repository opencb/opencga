package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.OpenArrayPharmacogenomicsAnalysisParams;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.PharmacogenomicsAnalysis;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.PharmacogenomicsAnalysisSummary;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualAnalysis;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * OpenArray pharmacogenomics analysis tool.
 * Executes the Python pharmacogenomics CLI via Docker to infer star alleles
 * from ThermoFisher OpenArray genotyping data.
 */
@Tool(id = OpenArrayPharmacogenomicsAnalysis.ID, resource = Enums.Resource.CLINICAL_ANALYSIS,
        description = OpenArrayPharmacogenomicsAnalysis.DESCRIPTION)
public class OpenArrayPharmacogenomicsAnalysis extends OpenCgaTool {

    public static final String ID = "openarray-pharmacogenomics";
    public static final String DESCRIPTION = "OpenArray pharmacogenomics analysis: infer star alleles from "
            + "ThermoFisher OpenArray genotyping data and optionally annotate with CPIC";

    private static final String STEP_EXECUTE = "execute";
    private static final String STEP_UPDATE_CATALOG = "update-catalog";

    @ToolParams
    protected final OpenArrayPharmacogenomicsAnalysisParams analysisParams = new OpenArrayPharmacogenomicsAnalysisParams();

    // Resolved physical file paths
    private String snvFilePath;
    private String translationFilePath;
    private String cnvFilePath;
    private String renameFilePath;
    private String compareToPath;

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(study)) {
            throw new ToolException("Missing study");
        }

        FileManager fileManager = catalogManager.getFileManager();

        // Resolve required files from catalog
        if (StringUtils.isEmpty(analysisParams.getSnvFile())) {
            throw new ToolException("Missing required parameter: snvFile");
        }
        snvFilePath = AnalysisUtils.getCatalogFile(analysisParams.getSnvFile(), study, fileManager, token)
                .getUri().getPath();

        if (StringUtils.isEmpty(analysisParams.getTranslationFile())) {
            throw new ToolException("Missing required parameter: translationFile");
        }
        translationFilePath = AnalysisUtils.getCatalogFile(analysisParams.getTranslationFile(), study, fileManager, token)
                .getUri().getPath();

        // Resolve optional files
        if (StringUtils.isNotEmpty(analysisParams.getCnvFile())) {
            cnvFilePath = AnalysisUtils.getCatalogFile(analysisParams.getCnvFile(), study, fileManager, token)
                    .getUri().getPath();
        }

        if (StringUtils.isNotEmpty(analysisParams.getRenameFile())) {
            renameFilePath = AnalysisUtils.getCatalogFile(analysisParams.getRenameFile(), study, fileManager, token)
                    .getUri().getPath();
        }

        if (StringUtils.isNotEmpty(analysisParams.getCompareToFile())) {
            compareToPath = AnalysisUtils.getCatalogFile(analysisParams.getCompareToFile(), study, fileManager, token)
                    .getUri().getPath();
        }

        setUpStorageEngineExecutor(study);
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(STEP_EXECUTE, STEP_UPDATE_CATALOG);
    }

    @Override
    protected void run() throws ToolException {
        // Step 1: Execute the Python pharmacogenomics tool via Docker
        step(STEP_EXECUTE, () -> {
            OpenArrayPharmacogenomicsAnalysisExecutor executor =
                    getToolExecutor(OpenArrayPharmacogenomicsAnalysisExecutor.class);

            executor.setSnvFilePath(snvFilePath)
                    .setTranslationFilePath(translationFilePath)
                    .setCnvFilePath(cnvFilePath)
                    .setRenameFilePath(renameFilePath)
                    .setCompareToPath(compareToPath)
                    .setAnnotate(Boolean.TRUE.equals(analysisParams.getAnnotate()))
                    .execute();
        });

        // Step 2: Read summary JSON files and update Individual entities in catalog
        step(STEP_UPDATE_CATALOG, this::updateCatalog);
    }

    /**
     * Read *_summary.json files from the output directory and update the corresponding
     * Individual entities in the catalog with the pharmacogenomics analysis results.
     */
    private void updateCatalog() throws IOException, CatalogException {
        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();
        Path outDir = getOutDir();
        String outDirPath = outDir.toAbsolutePath().toString();

        // Find all *_summary.json files in the output directory
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(outDir, "*_summary.json")) {
            for (Path summaryFile : stream) {
                String filename = summaryFile.getFileName().toString();
                // Extract sampleId: <sampleId>_summary.json
                String sampleId = filename.replace("_summary.json", "");

                // Skip control samples
                if ("NTC".equalsIgnoreCase(sampleId) || sampleId.startsWith("CALT")
                        || sampleId.startsWith("CHET") || sampleId.startsWith("CREF")) {
                    logger.debug("Skipping control sample: {}", sampleId);
                    continue;
                }

                // Read summary JSON
                PharmacogenomicsAnalysisSummary summary = objectMapper.readValue(
                        summaryFile.toFile(), PharmacogenomicsAnalysisSummary.class);

                // Build the full results file path (catalog-relative)
                String resultsFilePath = outDirPath + "/" + sampleId + ".json";

                // Build PharmacogenomicsAnalysis object
                PharmacogenomicsAnalysis pgxAnalysis = new PharmacogenomicsAnalysis(
                        sampleId, "openarray", resultsFilePath, summary);

                // Find the Individual that has this sample
                updateIndividualBySample(sampleId, pgxAnalysis);
            }
        }
    }

    /**
     * Find the Individual linked to the given sampleId and update its analysis.pharmacogenomics field.
     */
    private void updateIndividualBySample(String sampleId, PharmacogenomicsAnalysis pgxAnalysis) throws CatalogException {
        // Search for the sample to find the linked individual
        OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager()
                .get(study, sampleId, QueryOptions.empty(), token);

        if (sampleResult.getNumResults() == 0) {
            logger.warn("Sample '{}' not found in study '{}'. Skipping catalog update.", sampleId, study);
            return;
        }

        String individualId = sampleResult.first().getIndividualId();
        if (StringUtils.isEmpty(individualId)) {
            logger.warn("Sample '{}' is not linked to an individual. Skipping catalog update.", sampleId);
            return;
        }

        // Get current individual to preserve existing pharmacogenomics entries
        OpenCGAResult<Individual> individualResult = catalogManager.getIndividualManager()
                .get(study, individualId, QueryOptions.empty(), token);

        if (individualResult.getNumResults() == 0) {
            logger.warn("Individual '{}' not found in study '{}'. Skipping catalog update.", individualId, study);
            return;
        }

        Individual individual = individualResult.first();
        IndividualAnalysis analysis = individual.getAnalysis();
        if (analysis == null) {
            analysis = new IndividualAnalysis();
        }

        List<PharmacogenomicsAnalysis> pgxList = analysis.getPharmacogenomics();
        if (pgxList == null) {
            pgxList = new ArrayList<>();
        }

        // Remove any existing entry for the same sampleId+source to avoid duplicates
        pgxList.removeIf(existing -> sampleId.equals(existing.getSampleId())
                && "openarray".equals(existing.getSource()));

        // Add the new entry
        pgxList.add(pgxAnalysis);
        analysis.setPharmacogenomics(pgxList);

        // Update the individual
        IndividualUpdateParams updateParams = new IndividualUpdateParams();
        updateParams.setAnalysis(analysis);

        catalogManager.getIndividualManager().update(study, individualId, updateParams, QueryOptions.empty(), token);
        logger.info("Updated individual '{}' with pharmacogenomics results for sample '{}'", individualId, sampleId);
    }
}
