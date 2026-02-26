/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.core.cellbase.CellBaseValidator;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.AlleleTyperResult;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.StarAlleleAnnotation;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * PharmacogenomicsManager handles pharmacogenomics analysis workflow,
 * including allele typing and storing results in the catalog.
 */
public class PharmacogenomicsManager {

    private Logger logger = LoggerFactory.getLogger(PharmacogenomicsManager.class);

    private CatalogManager catalogManager;
    private ObjectMapper objectMapper;

    public PharmacogenomicsManager(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Perform allele typing on pharmacogenomics data and store results in catalog.
     *
     * @param studyId Study identifier
     * @param genotypingFileContent Genotyping file content as String
     * @param translationFileContent Translation file content as String
     * @param token Authentication token
     * @return List of AlleleTyperResult objects
     * @throws IOException if parsing fails
     * @throws CatalogException if catalog operations fail
     */
    public List<AlleleTyperResult> alleleTyper(String studyId, String genotypingFileContent,
                                                     String translationFileContent, String token)
            throws IOException, CatalogException {
        logger.info("Starting pharmacogenomics allele typing for study: {}", studyId);

        // First, CellBase validator
        CellBaseValidator cellBaseValidator = buildCellBaseValidator(studyId, token);
        if (cellBaseValidator == null) {
            throw new IOException("No CellBase configuration found for study " + studyId + ". Skipping star allele annotation.");
        }

        // Initialize AlleleTyper
        AlleleTyper typer = new AlleleTyper();

        // Parse translation file
        typer.parseTranslationFromString(translationFileContent);

        // Build pharmacogenomics results
        List<AlleleTyperResult> results = typer.buildAlleleTyperResultsFromString(genotypingFileContent);

        logger.info("Allele typing completed for {} samples", results.size());

        // Annotate star alleles with CellBase pharmacogenomics data
        annotateResults(results, cellBaseValidator.getCellBaseClient());

        // Store results in catalog for each sample
        storeResultsInCatalog(studyId, results, token);

        return results;
    }
    public List<AlleleTyperResult> alleleTyper(String studyId, String genotypingFileContent, String translationFileContent,
                                               boolean annotate, String token)
            throws IOException, CatalogException {
        logger.info("Starting pharmacogenomics allele typing for study: {}", studyId);

        // First, CellBase validator
        CellBaseValidator cellBaseValidator = buildCellBaseValidator(studyId, token);
        if (cellBaseValidator == null) {
            throw new IOException("No CellBase configuration found for study " + studyId + ". Skipping star allele annotation.");
        }

        // Initialize AlleleTyper
        AlleleTyper typer = new AlleleTyper();

        // Parse translation file
        typer.parseTranslationFromString(translationFileContent);

        // Build pharmacogenomics results
        List<AlleleTyperResult> results = typer.buildAlleleTyperResultsFromString(genotypingFileContent);
        logger.info("Allele typing completed for {} samples", results.size());

        if (annotate) {
            // Annotate star alleles with CellBase pharmacogenomics data
            annotateResults(results, cellBaseValidator.getCellBaseClient());
        }

//        // Store results in the path outdir: each sample in a JSON file
//        storeResultsInCatalog(studyId, results, token);

        return results;
    }

    /**
     * Annotate star alleles with CellBase pharmacogenomics data.
     * Skips annotation if no CellBase configuration is found for the project.
     */
    public void annotateResults(List<AlleleTyperResult> results, CellBaseClient cellBaseClient) throws IOException {
        logger.info("Annotating star alleles with CellBase pharmacogenomics data");
        StarAlleleAnnotator annotator = new StarAlleleAnnotator(cellBaseClient);
        long startTime = System.currentTimeMillis();
        for (AlleleTyperResult result : results) {
            if (result == null || result.getAlleleTyperResults() == null) {
                continue;
            }
            for (AlleleTyperResult.StarAlleleResult starAlleleResult : result.getAlleleTyperResults()) {
                String gene = starAlleleResult.getGene();
                if (gene == null || gene.isEmpty() || starAlleleResult.getAlleleCalls() == null) {
                    continue;
                }
                for (AlleleTyperResult.AlleleCall alleleCall : starAlleleResult.getAlleleCalls()) {
                    StarAlleleAnnotation annotation = annotator.annotate(gene, alleleCall.getAllele());
                    alleleCall.setAnnotation(annotation);
                }
            }
        }
        double annotationTime = (System.currentTimeMillis() - startTime) / 1000.0d;
        logger.info("Star allele annotation completed for {} samples in {} s", results.size(), annotationTime);
    }

    /**
     * Build a CellBaseValidator from the project's CellBase configuration.
     *
     * @return CellBaseValidator, or null if no configuration found
     */
    public CellBaseValidator buildCellBaseValidator(String studyId, String token) throws CatalogException {
        // Resolve study to FQN first
        Study study = catalogManager.getStudyManager()
                .get(Collections.singletonList(studyId), StudyManager.INCLUDE_STUDY_IDS, false, token)
                .first();
        String projectFqn = catalogManager.getStudyManager().getProjectFqn(study.getFqn());

        Project project = catalogManager.getProjectManager()
                .get(projectFqn, new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                        ProjectDBAdaptor.QueryParams.CELLBASE.key(),
                        ProjectDBAdaptor.QueryParams.ORGANISM.key())), token)
                .first();

        CellBaseConfiguration cellbase = project.getCellbase();
        ProjectOrganism organism = project.getOrganism();

        if (cellbase == null || organism == null) {
            return null;
        }

        String species = organism.getScientificName();
        String assembly = organism.getAssembly();
        return new CellBaseValidator(cellbase, species, assembly);
    }

    public void storeResultsInPath(List<AlleleTyperResult> results, Path annotatedDir) throws IOException {
        long totalSize = 0;
        for (AlleleTyperResult r : results) {
            byte[] json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(r);
            Path samplePath = annotatedDir.resolve(r.getSampleId() + ".json");
            Files.write(samplePath, json);
            totalSize += json.length;
        }
        logger.info("Allele typer results written to: {} ({}) files", annotatedDir, results.size());
        if (logger.isInfoEnabled()) {
            logger.info("Allele typer results total serialized size: {} KB", String.format("%.2f", totalSize / 1024.0));
        }
    }

    /**
     * Store pharmacogenomics results as attributes in sample objects.
     *
     * @param studyId Study identifier
     * @param results List of pharmacogenomics results
     * @param token Authentication token
     * @throws CatalogException if catalog operations fail
     * @throws IOException if JSON serialization fails
     */
    private void storeResultsInCatalog(String studyId, List<AlleleTyperResult> results, String token)
            throws CatalogException, IOException {
        logger.info("Storing pharmacogenomics results in catalog for {} samples", results.size());

        for (AlleleTyperResult result : results) {
            String sampleId = result.getSampleId();

            // Skip NTC or control samples
            if ("NTC".equalsIgnoreCase(sampleId)) {
                logger.debug("Skipping control sample: {}", sampleId);
                continue;
            }

            try {
                // Check if sample exists
                OpenCGAResult<org.opencb.opencga.core.models.sample.Sample> sampleResult =
                        catalogManager.getSampleManager().get(studyId, sampleId, QueryOptions.empty(), token);

                if (sampleResult.getNumResults() == 0) {
                    logger.warn("Sample {} not found in study {}. Skipping pharmacogenomics result storage.", sampleId, studyId);
                    continue;
                }

                // Convert result to Map for storage
                Map<String, Object> attributes = new HashMap<>();
                attributes.put("OPENCGA_PHARMACOGENOMICS", objectMapper.convertValue(result, Map.class));

                // Update sample with pharmacogenomics results
                SampleUpdateParams updateParams = new SampleUpdateParams();
                updateParams.setAttributes(attributes);

                catalogManager.getSampleManager().update(studyId, sampleId, updateParams, QueryOptions.empty(), token);
                logger.debug("Updated sample {} with pharmacogenomics results", sampleId);
            } catch (CatalogException e) {
                logger.error("Failed to update sample {} with pharmacogenomics results: {}", sampleId, e.getMessage());
                throw e;
            }
        }

        logger.info("Successfully stored pharmacogenomics results for {} samples in catalog", results.size());
    }

    /**
     * Get CatalogManager instance.
     *
     * @return CatalogManager
     */
    public CatalogManager getCatalogManager() {
        return catalogManager;
    }
}
