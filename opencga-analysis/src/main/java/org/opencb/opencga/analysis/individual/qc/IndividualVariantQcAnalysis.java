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
import com.fasterxml.jackson.core.type.TypeReference;
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
import org.opencb.opencga.core.models.common.QualityControlStatus;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualInternal;
import org.opencb.opencga.core.models.individual.IndividualQualityControl;
import org.opencb.opencga.core.models.variant.qc.IndividualQcAnalysisParams;
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

import static org.opencb.opencga.core.models.common.InternalStatus.READY;
import static org.opencb.opencga.core.models.common.QualityControlStatus.ERROR;
import static org.opencb.opencga.core.models.study.StudyPermissions.Permissions.WRITE_INDIVIDUALS;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.JSON;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.VCF_GZ;

@Tool(id = IndividualVariantQcAnalysis.ID, resource = Enums.Resource.SAMPLE, description = IndividualVariantQcAnalysis.DESCRIPTION)
public class IndividualVariantQcAnalysis extends VariantQcAnalysis {

    public static final String ID = "individual-variant-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given individual. This includes inferred sex, Mendelian"
            + " errors (UDP), and, if parents are present, a relatedness analysis is also performed";

    @ToolParams
    protected final IndividualQcAnalysisParams analysisParams = new IndividualQcAnalysisParams();

    // Individuals to perform QC and VCF and JSON files
    private List<Individual> individuals = new ArrayList<>();

    // Set of individuals with known parents, i.e., trios
    private Set<Individual> trios = new HashSet<>();


    @Override
    protected void check() throws Exception {
        setUpStorageEngineExecutor(study);

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

        // Check custom resources path
        userResourcesPath = checkResourcesDir(analysisParams.getResourcesDir(), getStudy(), catalogManager, token);
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = Arrays.asList(PREPARE_QC_STEP, ID);
        if (!Boolean.TRUE.equals(analysisParams.getSkipIndex())) {
            steps.add(INDEX_QC_STEP);
        }
        return steps;
    }

    @Override
    protected void run() throws ToolException {
        // Main steps
        step(PREPARE_QC_STEP, this::prepareQualityControl);
        step(ID, this::runIndividualQc);
        if (getSteps().contains(INDEX_QC_STEP)) {
            step(INDEX_QC_STEP, this::indexQualityControl);
        }

        // Clean execution
        clean();
    }

    protected void prepareQualityControl() throws ToolException {
        try {
            ObjectWriter objectWriter = JacksonUtils.getDefaultObjectMapper().writerFor(Individual.class);
            for (String individualId : analysisParams.getIndividuals()) {
                // Get individual
                Individual individual = catalogManager.getIndividualManager().get(study, individualId, QueryOptions.empty(), token).first();

                // Add family to the list
                individuals.add(individual);

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
                    String gt = getIndexedAndNoSomaticSampleIds(individual).stream().map(s -> s + ":0/0,0/1,1/1")
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
                String basename = getOutDir().resolve(individualId).toAbsolutePath().toString();
                getVariantStorageManager().exportData(basename, VCF_GZ, null, query, queryOptions, token);

                // Check VCF file
                Path vcfPath = Paths.get(basename + "." + VCF_GZ.getExtension());
                if (!Files.exists(vcfPath)) {
                    throw new ToolException("Something wrong happened when exporting VCF file for individual ID '" + individualId
                            + "'. VCF file " + vcfPath + " was not created. Export query = " + query.toJson() + "; export query"
                            + " options = " + queryOptions.toJson());
                }
                vcfPaths.add(vcfPath);

                // Export individual (JSON format)
                Path jsonPath = Paths.get(basename + "." + JSON.getExtension());
                objectWriter.writeValue(jsonPath.toFile(), individual);

                // Check VCF file
                if (!Files.exists(jsonPath)) {
                    throw new ToolException("Something wrong happened when saving JSON file for individual ID '" + individualId
                            + "'. JSON file " + jsonPath + " was not created.");
                }
                jsonPaths.add(jsonPath);
            }
        } catch (CatalogException | IOException | StorageEngineException e) {
            // Clean execution
            clean();
            throw new ToolException(e);
        }
    }

    protected void runIndividualQc() throws ToolException {
        // Get executor to execute Python script that computes the family QC
        IndividualVariantQcAnalysisExecutor executor = getToolExecutor(IndividualVariantQcAnalysisExecutor.class);
        executor.setVcfPaths(vcfPaths)
                .setJsonPaths(jsonPaths)
                .setQcParams(analysisParams)
                .execute();
    }

    private void indexQualityControl() throws ToolException {
        // Create readers for each QC report
        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        ObjectReader inferredSexReader = JacksonUtils.getDefaultObjectMapper().readerFor(InferredSex.class);
        ObjectReader inferredSexListReader = JacksonUtils.getDefaultObjectMapper().readerFor(new TypeReference<List<InferredSex>>() {});
        ObjectReader mendelianErrorReader = JacksonUtils.getDefaultObjectMapper().readerFor(MendelianError.class);
        ObjectReader mendelianErrorListReader = JacksonUtils.getDefaultObjectMapper().readerFor(
                new TypeReference<List<MendelianError>>() {});
        ObjectReader relatednessReader = JacksonUtils.getDefaultObjectMapper().readerFor(Relatedness.class);
        ObjectReader relatednessListReader = JacksonUtils.getDefaultObjectMapper().readerFor(new TypeReference<List<Relatedness>>() {});

        QualityControlStatus qcStatus;
        IndividualQualityControl individualQc;
        Set<String> failedAnalysis = new HashSet<>();
        String logMsg;

        failedQcSet = new HashSet<>();
        for (Individual individual : individuals) {
            individualQc = new IndividualQualityControl();

            // Check and parse the inferred sex results
            Path qcPath = getOutDir().resolve(individual.getId()).resolve(INFERRED_SEX_ANALYSIS_ID).resolve(QC_RESULTS_FILENAME);
            if (!Files.exists(qcPath)) {
                failedQcSet.add(individual.getId());
                failedAnalysis.add(INFERRED_SEX_ANALYSIS_ID);
                individualQc.setInferredSex(Collections.emptyList());

                logMsg = FAILURE_FILE + qcPath.getFileName() + NOT_FOUND + getIdLogMessage(individual.getId(), INDIVIDUAL_QC_TYPE);
                addError(new ToolException(logMsg));
                logger.error(logMsg);
            } else {
                try {
                    List<InferredSex> inferredSexList = isQcArray(qcPath)
                            ? inferredSexListReader.readValue(qcPath.toFile())
                            : Collections.singletonList(inferredSexReader.readValue(qcPath.toFile()));

                    individualQc.setInferredSex(inferredSexList);
                } catch (IOException e) {
                    failedQcSet.add(individual.getId());
                    failedAnalysis.add(INFERRED_SEX_ANALYSIS_ID);
                    individualQc.setInferredSex(Collections.emptyList());

                    logMsg = FAILURE_ERROR_PARSING_QC_JSON_FILE + qcPath.getFileName() + "'" + getIdLogMessage(individual.getId(),
                            INDIVIDUAL_QC_TYPE);
                    addError(e);
                    logger.error(logMsg, e);
                }
            }

            // Check and parse the mendelian error results
            qcPath = getOutDir().resolve(individual.getId()).resolve(MENDELIAN_ERROR_ANALYSIS_ID).resolve(QC_RESULTS_FILENAME);
            if (!Files.exists(qcPath)) {
                failedQcSet.add(individual.getId());
                failedAnalysis.add(MENDELIAN_ERROR_ANALYSIS_ID);
                individualQc.setInferredSex(Collections.emptyList());

                logMsg = FAILURE_FILE + qcPath.getFileName() + NOT_FOUND + getIdLogMessage(individual.getId(), INDIVIDUAL_QC_TYPE);
                addError(new ToolException(logMsg));
                logger.error(logMsg);
            } else {
                try {
                    List<MendelianError> mendelianErrorList = isQcArray(qcPath)
                            ? mendelianErrorListReader.readValue(qcPath.toFile())
                            : Collections.singletonList(mendelianErrorReader.readValue(qcPath.toFile()));

                    individualQc.setMendelianError(mendelianErrorList);
                } catch (IOException e) {
                    failedQcSet.add(individual.getId());
                    failedAnalysis.add(MENDELIAN_ERROR_ANALYSIS_ID);
                    individualQc.setMendelianError(Collections.emptyList());

                    logMsg = FAILURE_ERROR_PARSING_QC_JSON_FILE + qcPath.getFileName() + "'" + getIdLogMessage(individual.getId(),
                            INDIVIDUAL_QC_TYPE);
                    addError(e);
                    logger.error(logMsg, e);
                }
            }

            // Check and parse the relatedness results, if trio is present
            if (CollectionUtils.isNotEmpty(trios) && trios.contains(individual)) {
                qcPath = getOutDir().resolve(individual.getId()).resolve(RELATEDNESS_ANALYSIS_ID).resolve(QC_RESULTS_FILENAME);
                if (!Files.exists(qcPath)) {
                    failedQcSet.add(individual.getId());
                    failedAnalysis.add(RELATEDNESS_ANALYSIS_ID);
                    individualQc.setRelatedness(Collections.emptyList());

                    logMsg = FAILURE_FILE + qcPath.getFileName() + NOT_FOUND + getIdLogMessage(individual.getId(), INDIVIDUAL_QC_TYPE);
                    addError(new ToolException(logMsg));
                    logger.error(logMsg);
                } else {
                    try {
                        List<Relatedness> relatednessList = isQcArray(qcPath)
                                ? relatednessListReader.readValue(qcPath.toFile())
                                : Collections.singletonList(relatednessReader.readValue(qcPath.toFile()));

                        individualQc.setRelatedness(relatednessList);
                    } catch (IOException e) {
                        failedQcSet.add(individual.getId());
                        failedAnalysis.add(RELATEDNESS_ANALYSIS_ID);
                        individualQc.setRelatedness(Collections.emptyList());

                        logMsg = FAILURE_ERROR_PARSING_QC_JSON_FILE + qcPath.getFileName() + "'" + getIdLogMessage(individual.getId(),
                                INDIVIDUAL_QC_TYPE);
                        addError(e);
                        logger.error(logMsg, e);
                    }
                }
            }

            if (CollectionUtils.isEmpty(failedAnalysis)) {
                qcStatus = new QualityControlStatus(READY, SUCCESS);
            } else {
                qcStatus = new QualityControlStatus(ERROR, "Failed analysis: " + StringUtils.join(failedAnalysis, ","));
            }

            try {
                catalogManager.getIndividualManager().updateQualityControl(getStudy(), individual.getId(), individualQc, qcStatus, token);
            } catch (CatalogException e) {
                failedQcSet.add(individual.getId());
                logMsg = FAILURE_COULD_NOT_UPDATE_QUALITY_CONTROL_IN_OPEN_CGA_CATALOG + getIdLogMessage(individual.getId(),
                        INDIVIDUAL_QC_TYPE);
                addError(e);
                logger.error(logMsg, e);
            }
        }

        checkFailedQcCounter(individuals.size(), INDIVIDUAL_QC_TYPE);
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
        checkPermissions(WRITE_INDIVIDUALS, params.getSkipIndex(), studyId, catalogManager, token);

        if (!Boolean.TRUE.equals(params.getSkipIndex()) && CollectionUtils.isNotEmpty(params.getSkipAnalysis())) {
            throw new ToolException("To index QC in the OpenCGA catalog, the 'skip analysis' parameter must be left empty");
        }

        // Check individuals in catalog
        // Collect possible errors in a map where key is the family ID and value is the error
        Map<String, String> errors = new HashMap<>();
        for (String individualId : params.getIndividuals()) {
            // Get Individual from catalog
            Individual individual = null;
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
                    List<String> sampleIds = getIndexedAndNoSomaticSampleIds(individual);
                    if (CollectionUtils.isEmpty(sampleIds)) {
                        errors.put(individualId, "None of the input individuals have indexed and somatic samples");
                    }
                }

                // Check compatibility between QC status (READY) and overwrite
                if (!Boolean.TRUE.equals(params.getSkipIndex()) && !Boolean.TRUE.equals(params.getOverwrite())
                        && Optional.ofNullable(individual)
                        .map(Individual::getInternal)
                        .map(IndividualInternal::getQualityControlStatus)
                        .map(QualityControlStatus::getId)
                        .filter(READY::equals)
                        .isPresent()) {
                    errors.put(individualId, "It is mandatory to set 'overwrite' to TRUE when QC has already been computed"
                            + " (status: READY)");
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
