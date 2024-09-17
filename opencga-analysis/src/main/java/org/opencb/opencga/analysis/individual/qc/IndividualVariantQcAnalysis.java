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

package org.opencb.opencga.analysis.individual.qc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.InferredSex;
import org.opencb.biodata.models.clinical.qc.MendelianError;
import org.opencb.biodata.models.clinical.qc.Relatedness;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.qc.VariantQcAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualQualityControl;
import org.opencb.opencga.core.models.individual.IndividualQualityControlStatus;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.variant.IndividualQcAnalysisParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.IndividualVariantQcAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.models.common.QualityControlStatus.COMPUTING;
import static org.opencb.opencga.core.models.individual.IndividualQualityControlStatus.*;
import static org.opencb.opencga.core.models.study.StudyPermissions.Permissions.WRITE_INDIVIDUALS;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.JSON;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.VCF_GZ;

@Tool(id = IndividualVariantQcAnalysis.ID, resource = Enums.Resource.SAMPLE, description = IndividualVariantQcAnalysis.DESCRIPTION)
public class IndividualVariantQcAnalysis extends VariantQcAnalysis {

    public static final String ID = "individual-variant-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given individual. This includes inferred sex, Mendelian"
            + " errors (UDP), and, if parents are present, a relatedness analysis is also performed";

    // Set of individuals with known parents, i.e., trios
    private Set<Individual> trios = new HashSet<>();

    @ToolParams
    protected final IndividualQcAnalysisParams analysisParams = new IndividualQcAnalysisParams();

    @Override
    protected void check() throws Exception {
        super.check();
        checkParameters(analysisParams, getStudy(), catalogManager, token);

        // Check for the presence of trios to compute relatedness, and then prepare relatedness resource files
        for (String individualId : analysisParams.getIndividuals()) {
            // Get individual
            Individual individual = catalogManager.getIndividualManager().get(study, individualId, QueryOptions.empty(), token).first();
            if (individual.getFather() != null && individual.getMother() != null) {
                trios.add(individual);
            }
        }
        if (CollectionUtils.isNotEmpty(trios)) {
            prepareRelatednessResources(analysisParams.getResourcesDir());
        }

        // Prepare inferred sex resource files
        prepareInferredSexResources(analysisParams.getResourcesDir());
    }

    @Override
    protected void run() throws ToolException {
        setUpStorageEngineExecutor(study);

        List<Individual> individuals = new ArrayList<>();
        LinkedList<Path> individualVcfPaths = new LinkedList<>();
        LinkedList<Path> individualJsonPaths = new LinkedList<>();

        try {
            ObjectWriter objectWriter = JacksonUtils.getDefaultObjectMapper().writerFor(Individual.class);
            for (String individualId : analysisParams.getIndividuals()) {
                // Get individual
                Individual individual = catalogManager.getIndividualManager().get(study, individualId, QueryOptions.empty(), token).first();

                // Decide if quality control has to be performed
                //   - by checking the quality control status, if it is READY means it has been computed previously, and
                //   - by checking the flag overwrite
                if (individual.getInternal() == null || mustPerformQualityControl(individual.getInternal().getQualityControlStatus(),
                        analysisParams.getOverwrite())) {

                    // Set quality control status to COMPUTING to prevent multiple individual QCs from running simultaneously
                    // for the same individual
                    IndividualQualityControlStatus qcStatus = new IndividualQualityControlStatus(COMPUTING,
                            "Performing " + INDIVIDUAL_QC_TYPE + " QC");
                    if (!setQualityControlStatus(qcStatus, individual.getId(), INDIVIDUAL_QC_TYPE)) {
                        continue;
                    }

                    // Create directory to save variants and individual files
                    Path individualOutPath = Files.createDirectories(getOutDir().resolve(individualId));
                    if (!Files.exists(individualOutPath)) {
                        throw new ToolException("Error creating directory: " + individualOutPath);
                    }

                    // Export individual variants (VCF format)
                    // Create the query based on whether a trio is present or not
                    Query query;
                    if (trios.contains(individual)) {
                        String child = individual.getSamples().get(0).getId();
                        String father = individual.getFather().getSamples().get(0).getId();
                        String mother = individual.getMother().getSamples().get(0).getId();
                        query = new Query()
                                .append(VariantQueryParam.SAMPLE.key(), child + ":0/1,1/1")
                                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), child + "," + father + "," + mother)
                                .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT");
                    } else {
                        // Create variant query
                        String gt = getNoSomaticSampleIds(individual).stream().map(s -> s + ":0/0,0/1,1/1")
                                .collect(Collectors.joining(";"));
                        query = new Query()
                                .append(VariantQueryParam.GENOTYPE.key(), gt);
                    }
                    query.append(VariantQueryParam.STUDY.key(), study)
                            .append(VariantQueryParam.TYPE.key(), VariantType.SNV)
                            .append(VariantQueryParam.REGION.key(), Arrays.asList("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22"
                                    .split(",")));

                    // Create query options
                    QueryOptions queryOptions = new QueryOptions().append(QueryOptions.INCLUDE, "id,studies.samples");

                    // Export to VCF.GZ format
                    String basename = individualOutPath.resolve(individualId).toAbsolutePath().toString();
                    getVariantStorageManager().exportData(basename, VCF_GZ, null, query, queryOptions, token);

                    // Check VCF file
                    Path individualVcfPath = Paths.get(basename + "." + VCF_GZ.getExtension());
                    if (!Files.exists(individualVcfPath)) {
                        throw new ToolException("Something wrong happened when exporting VCF file for individual ID '" + individualId
                                + "'. VCF file " + individualVcfPath + " was not created. Export query = " + query.toJson() + "; export query"
                                + " options = " + queryOptions.toJson());
                    }
                    individualVcfPaths.add(individualVcfPath);

                    // Export individual (JSON format)
                    Path individualJsonPath = Paths.get(basename + "." + JSON.getExtension());
                    objectWriter.writeValue(individualJsonPath.toFile(), individual);

                    // Check VCF file
                    if (!Files.exists(individualJsonPath)) {
                        throw new ToolException("Something wrong happened when saving JSON file for individual ID '" + individualId
                                + "'. JSON file " + individualJsonPath + " was not created.");
                    }
                    individualJsonPaths.add(individualJsonPath);

                    // Add family to the list
                    individuals.add(individual);
                }
            }
        } catch (CatalogException | IOException | StorageEngineException e) {
            throw new ToolException(e);
        }

        // Get executor to execute Python script that computes the family QC
        IndividualVariantQcAnalysisExecutor executor = getToolExecutor(IndividualVariantQcAnalysisExecutor.class);
        executor.setVcfPaths(individualVcfPaths)
                .setJsonPaths(individualJsonPaths)
                .setQcParams(analysisParams);

        // Step by step
        step(executor::execute);

        // Parse Python script results
        if (!Boolean.TRUE.equals(analysisParams.getSkipIndex())) {
            updateIndividualQualityControl(individuals);
        }
    }

    /**
     * Update quality control for each individual by parsing the QC report for the inferred sex, Mendelian errors and relatedness analyses.
     *
     * @param individuals List of individuals
     * @throws ToolException Tool exception
     */
    private void updateIndividualQualityControl(List<Individual> individuals) throws ToolException {
        // Create readers for each QC report
        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        ObjectReader inferredSexReader = JacksonUtils.getDefaultObjectMapper().readerFor(InferredSex.class);
        ObjectReader mendelianErrorReader = JacksonUtils.getDefaultObjectMapper().readerFor(MendelianError.class);
        ObjectReader relatednessReader = JacksonUtils.getDefaultObjectMapper().readerFor(Relatedness.class);

        for (Individual individual : individuals) {
            // Check output files
            Path qcPath = getOutDir().resolve(individual.getId());
            if (!Files.exists(qcPath)) {
                String msg = "Quality control error for individual " + individual.getId() + ": folder " + qcPath + " not found."
                        + " None quality control was performed.";
                logger.error(msg);
                addError(new ToolException(msg));
                continue;
            }

            int qcCode = NONE_READY;
            IndividualQualityControl individualQc = individual.getQualityControl();

            // Check inferred sex report
            InferredSex inferredSex = checkQcReport(individual.getId(), INFERRED_SEX_ANALYSIS_ID, analysisParams.getSkip(), qcPath,
                    INDIVIDUAL_QC_TYPE, inferredSexReader);
            if (inferredSex != null) {
                qcCode |= INFERRED_SEX_READY;
                individualQc.getInferredSex().add(inferredSex);
            }

            // Check Mendelian error report
            MendelianError mendelianError = checkQcReport(individual.getId(), MENDELIAN_ERROR_ANALYSIS_ID, analysisParams.getSkip(), qcPath,
                    INDIVIDUAL_QC_TYPE, mendelianErrorReader);
            if (mendelianError != null) {
                qcCode |= MENDELIAN_ERROR_READY;
                individualQc.getMendelianError().add(mendelianError);
            }

            // Check relatedness report
            Relatedness relatedness = checkQcReport(individual.getId(), RELATEDNESS_ANALYSIS_ID, analysisParams.getSkip(), qcPath,
                    INDIVIDUAL_QC_TYPE, relatednessReader);
            if (relatedness != null) {
                qcCode |= RELATEDNESS_READY;
                individualQc.getRelatedness().add(relatedness);
            }

            // Update catalog (quality control and status) if necessary
            if (qcCode != NONE_READY) {
                // Update the individual QC code with the current one
                IndividualQualityControlStatus qcStatus = new IndividualQualityControlStatus(
                        qcCode | individual.getInternal().getQualityControlStatus().getCode(), "");
                try {
                    IndividualUpdateParams updateParams = new IndividualUpdateParams()
                            .setQualityControl(individualQc)
                            .setQualityControlStatus(qcStatus);
                    catalogManager.getIndividualManager().update(getStudy(), individual.getId(), updateParams, null, token);
                } catch (CatalogException e) {
                    logger.error("Could not update quality control in OpenCGA catalog for individual " + individual.getId(), e);
                    addError(e);
                }
            }
        }
    }

    /**
     * Check individual QC parameters before submitting the QC job, including catalog permissions
     * IMPORTANT: it is a static method to be called by the WSServer before submitting the QC job.
     *
     * @param params Individual QC parameters
     * @param studyId Study ID
     * @param catalogManager Catalog manager
     * @param token Token to access OpenCGA catalog
     * @throws ToolException Tool exception
     */
    public static void checkParameters(IndividualQcAnalysisParams params, String studyId, CatalogManager catalogManager, String token)
            throws ToolException {
        // Check study
        checkStudy(studyId, catalogManager, token);

        // Check permissions
        checkPermissions(WRITE_INDIVIDUALS, studyId, catalogManager, token);

        // Check individuals in catalog
        // Collect possible errors in a map where key is the family ID and value is the error
        Map<String, String> errors = new HashMap<>();
        for (String individualId : params.getIndividuals()) {
            // Get Individual from catalog
            Individual individual;
            try {
                OpenCGAResult<Individual> individualResult = catalogManager.getIndividualManager().get(studyId, individualId,
                        QueryOptions.empty(), token);
                if (individualResult.getNumResults() == 0) {
                    errors.put(individualId, "Individual not found");
                } else if (individualResult.getNumResults() > 1) {
                    errors.put(individualId, "More than one individual found");
                } else {
                    individual = individualResult.first();

                    // Check number of samples
                    List<String> sampleIds = getNoSomaticSampleIds(individual);
                    if (CollectionUtils.isEmpty(sampleIds)) {
                        errors.put(individualId, "No samples found");
                    }
                }
            } catch (CatalogException e) {
                errors.put(individualId, Arrays.toString(e.getStackTrace()));
            }
        }

        // Check error
        if (MapUtils.isNotEmpty(errors)) {
            throw new ToolException("Found the following error for individual IDs: " + StringUtils.join(errors.entrySet().stream().map(
                    e -> "Individual ID " + e.getKey() + ": " + e.getValue()).collect(Collectors.toList()), ","));
        }

        // Check resources dir
        checkResourcesDir(params.getResourcesDir(), studyId, catalogManager, token);
    }
}
