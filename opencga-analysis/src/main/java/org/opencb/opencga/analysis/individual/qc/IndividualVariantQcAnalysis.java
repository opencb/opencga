package org.opencb.opencga.analysis.individual.qc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.InferredSex;
import org.opencb.biodata.models.clinical.qc.MendelianError;
import org.opencb.biodata.models.clinical.qc.Relatedness;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.qc.VariantQcAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.QualityControlStatus;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualInternal;
import org.opencb.opencga.core.models.individual.IndividualQualityControl;
import org.opencb.opencga.core.models.variant.qc.IndividualQcAnalysisParams;
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
    public static final String DESCRIPTION = "Run quality control (QC) for a given individual. This includes inferred sex, and if parents"
            + "  are present, Mendelian errors (UDP) and relatedness analyses are also performed";

    @ToolParams
    protected final IndividualQcAnalysisParams analysisParams = new IndividualQcAnalysisParams();

    // Individuals to perform QC and VCF and JSON files
    private Individual individual;

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        logger.info("Checking {}", analysisParams);

        checkParameters(analysisParams, study, catalogManager, token);

        // Get individual and check for the presence of trios to compute relatedness, and then prepare relatedness resource files
        individual = catalogManager.getIndividualManager().get(study, analysisParams.getIndividual(), QueryOptions.empty(), token).first();
        if (individual.getFather() != null && individual.getMother() != null) {
            Individual mother = catalogManager.getIndividualManager().get(study, individual.getMother().getId(), QueryOptions.empty(),
                    token).first();
            Individual father = catalogManager.getIndividualManager().get(study, individual.getFather().getId(), QueryOptions.empty(),
                    token).first();
            if (CollectionUtils.isNotEmpty(individual.getSamples())
                    && CollectionUtils.isNotEmpty(mother.getSamples())
                    && CollectionUtils.isNotEmpty(father.getSamples())) {
                individual.setMother(mother);
                individual.setFather(father);
            }
        }

        // Check custom relatedness resources: prune-in, frq and thresholds files
        userResourcesPath = checkUserResourcesDir(analysisParams.getResourcesDir(), study, catalogManager, token);
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>(Arrays.asList(PREPARE_RESOURCES_STEP, PREPARE_QC_STEP, ID));
        if (!Boolean.TRUE.equals(analysisParams.getSkipIndex())) {
            steps.add(INDEX_QC_STEP);
        }
        return steps;
    }

    @Override
    protected void run() throws ToolException {
        // Main steps
        step(PREPARE_RESOURCES_STEP, this::prepareResources);
        step(PREPARE_QC_STEP, this::prepareQualityControl);
        step(ID, this::runIndividualQc);
        if (getSteps().contains(INDEX_QC_STEP)) {
            step(INDEX_QC_STEP, this::indexQualityControl);
        }

        // Clean execution
        clean();
    }

    protected void prepareResources() throws IOException, ResourceException, ToolException {
        ResourceManager resourceManager = new ResourceManager(getOpencgaHome());

        // Prepare relatedness resources
        prepareResources(RELATEDNESS_ANALYSIS_ID, null, resourceManager);

        // Prepare inferred-sex resources
        prepareResources(INFERRED_SEX_ANALYSIS_ID, null, resourceManager);
    }

    protected void prepareQualityControl() throws ToolException {
        try {
            ObjectWriter objectWriter = JacksonUtils.getDefaultObjectMapper().writerFor(Individual.class);

            // Export individual variants (VCF format)
            // Create the query based on whether a trio is present or not
            Query query;
            if (CollectionUtils.isNotEmpty(individual.getMother().getSamples())
                    && CollectionUtils.isNotEmpty(individual.getFather().getSamples())) {
                // Create variant query for trio
                String childSample = individual.getSamples().get(0).getId();
                String fatherSample = individual.getFather().getSamples().get(0).getId();
                String motherSample = individual.getMother().getSamples().get(0).getId();
                query = new Query()
                        .append(VariantQueryParam.SAMPLE.key(), childSample + ":0/1,1/1")
                        .append(VariantQueryParam.INCLUDE_SAMPLE.key(), childSample + "," + fatherSample + "," + motherSample)
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
            String basename = getOutDir().resolve(individual.getId()).toAbsolutePath().toString();
            getVariantStorageManager().exportData(basename, VCF_GZ, null, query, queryOptions, token);

            // Check VCF file
            vcfPath = Paths.get(basename + "." + VCF_GZ.getExtension());
            if (!Files.exists(vcfPath)) {
                throw new ToolException("Something wrong happened when exporting VCF file for individual ID '" + individual.getId()
                        + "'. VCF file " + vcfPath + " was not created. Export query = " + query.toJson() + "; export query"
                        + " options = " + queryOptions.toJson());
            }

            // Export individual (JSON format)
            jsonPath = Paths.get(basename + "." + JSON.getExtension());
            objectWriter.writeValue(jsonPath.toFile(), individual);

            // Check VCF file
            if (!Files.exists(jsonPath)) {
                throw new ToolException("Something wrong happened when saving JSON file for individual ID '" + individual.getId()
                        + "'. JSON file " + jsonPath + " was not created.");
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
        executor.setVcfPath(vcfPath)
                .setJsonPath(jsonPath)
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
                new TypeReference<List<MendelianError>>() {
                });
        ObjectReader relatednessReader = JacksonUtils.getDefaultObjectMapper().readerFor(Relatedness.class);
        ObjectReader relatednessListReader = JacksonUtils.getDefaultObjectMapper().readerFor(new TypeReference<List<Relatedness>>() {
        });

        QualityControlStatus qcStatus;
        IndividualQualityControl individualQc = new IndividualQualityControl();

        String logMsg;
        Set<String> failedAnalysis = new HashSet<>();

        // Check and parse the inferred sex results
        Path qcPath = getOutDir().resolve(INFERRED_SEX_ANALYSIS_ID).resolve(QC_RESULTS_FILENAME);
        if (!Files.exists(qcPath)) {
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
                failedAnalysis.add(INFERRED_SEX_ANALYSIS_ID);
                individualQc.setInferredSex(Collections.emptyList());

                logMsg = FAILURE_ERROR_PARSING_QC_JSON_FILE + qcPath.getFileName() + "'" + getIdLogMessage(individual.getId(),
                        INDIVIDUAL_QC_TYPE);
                addError(e);
                logger.error(logMsg, e);
            }
        }

        // Check and parse the mendelian error results
        qcPath = getOutDir().resolve(MENDELIAN_ERROR_ANALYSIS_ID).resolve(QC_RESULTS_FILENAME);
        if (!Files.exists(qcPath)) {
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
                failedAnalysis.add(MENDELIAN_ERROR_ANALYSIS_ID);
                individualQc.setMendelianError(Collections.emptyList());

                logMsg = FAILURE_ERROR_PARSING_QC_JSON_FILE + qcPath.getFileName() + "'" + getIdLogMessage(individual.getId(),
                        INDIVIDUAL_QC_TYPE);
                addError(e);
                logger.error(logMsg, e);
            }
        }

        // Check and parse the relatedness results, if trio is present
        if (CollectionUtils.isNotEmpty(individual.getMother().getSamples())
                && CollectionUtils.isNotEmpty(individual.getFather().getSamples())) {
            qcPath = getOutDir().resolve(RELATEDNESS_ANALYSIS_ID).resolve(QC_RESULTS_FILENAME);
            if (!Files.exists(qcPath)) {
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
                    failedAnalysis.add(RELATEDNESS_ANALYSIS_ID);
                    individualQc.setRelatedness(Collections.emptyList());

                    logMsg = FAILURE_ERROR_PARSING_QC_JSON_FILE + qcPath.getFileName() + "'" + getIdLogMessage(individual.getId(),
                            INDIVIDUAL_QC_TYPE);
                    addError(e);
                    logger.error(logMsg, e);
                }
            }
        }

        // Set the quality control status
        if (CollectionUtils.isEmpty(failedAnalysis)) {
            qcStatus = new QualityControlStatus(READY, SUCCESS);
        } else {
            qcStatus = new QualityControlStatus(ERROR, "Failed analyses: " + StringUtils.join(failedAnalysis, ","));
        }

        try {
            catalogManager.getIndividualManager().updateQualityControl(study, individual.getId(), individualQc, qcStatus, token);
        } catch (CatalogException e) {
            throw new ToolException(FAILURE_COULD_NOT_UPDATE_QUALITY_CONTROL_IN_OPEN_CGA_CATALOG + getIdLogMessage(individual.getId(),
                    INDIVIDUAL_QC_TYPE), e);
        }

        if (ERROR.equalsIgnoreCase(qcStatus.getId())) {
            clean();
            throw new ToolException(qcStatus.getDescription());
        }
    }

    /**
     * Check individual QC parameters before submitting the QC job, including catalog permissions
     * IMPORTANT: it is a static method to be called by the WSServer before submitting the QC job.
     *
     * @param params         Individual QC parameters
     * @param studyId        Study ID
     * @param catalogManager Catalog manager
     * @param token          Token to access OpenCGA catalog
     * @throws ToolException Tool exception
     */
    public static void checkParameters(IndividualQcAnalysisParams params, String studyId, CatalogManager catalogManager, String token)
            throws ToolException, CatalogException {
        // Check study
        checkStudy(studyId, catalogManager, token);

        // Check permissions
        checkPermissions(WRITE_INDIVIDUALS, params.getSkipIndex(), studyId, catalogManager, token);

        String individualId = params.getIndividual();
        Individual individual = catalogManager.getIndividualManager().get(studyId, individualId, QueryOptions.empty(), token).first();

        // Check indexed and no-somatic samples
        List<String> sampleIds = getIndexedAndNoSomaticSampleIds(individual);
        if (CollectionUtils.isEmpty(sampleIds)) {
            throw new ToolException("Any indexed and no-somatic samples found for individual '" + individualId + "'");
        }

        // Check compatibility between QC status (READY) and overwrite
        if (!Boolean.TRUE.equals(params.getSkipIndex()) && !Boolean.TRUE.equals(params.getOverwrite())
                && Optional.ofNullable(individual).map(Individual::getInternal).map(IndividualInternal::getQualityControlStatus)
                .map(QualityControlStatus::getId).filter(READY::equals).isPresent()) {
            throw new ToolException("Individual QC " + HAS_ALREADY_BEEN_COMPUTED_WITH_A_READY_STATUS + " for individual '" + individualId
                    + "'. To recompute QC, you must set the 'overwrite' flag to TRUE.");
        }
    }
}
