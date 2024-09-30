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

package org.opencb.opencga.analysis.sample.qc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.GenomePlot;
import org.opencb.biodata.models.clinical.qc.Signature;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.qc.VariantQcAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.QualityControlStatus;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.variant.SampleVariantStatsAnalysisParams;
import org.opencb.opencga.core.models.variant.qc.SampleQcAnalysisParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.SampleVariantQcAnalysisExecutor;
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
import static org.opencb.opencga.core.models.study.StudyPermissions.Permissions.WRITE_SAMPLES;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.JSON;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.VCF_GZ;

@Tool(id = SampleVariantQcAnalysis.ID, resource = Enums.Resource.SAMPLE, description = SampleVariantQcAnalysis.DESCRIPTION)
public class SampleVariantQcAnalysis extends VariantQcAnalysis {

    public static final String ID = "sample-variant-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given sample. It includes variant stats, and if the sample " +
            "is somatic, mutational signature and genome plot are calculated.";

    private static final String STATS_QC_STEP = SampleVariantStatsAnalysis.ID + "-qc";
    private static final String SOMATIC_QC_STEP = "somatic-qc";

    @ToolParams
    protected final SampleQcAnalysisParams analysisParams = new SampleQcAnalysisParams();

    // Samples
    private List<Sample> samples = new ArrayList<>();

    // List of somatic samples, for these we need to create VCF and JSON files
    private LinkedList<Sample> targetSomaticSamples = new LinkedList<>();

    Map<String, Set<String>> failedAnalysisPerSample = new HashMap<>();

    @Override
    protected void check() throws Exception {
        setUpStorageEngineExecutor(study);

        super.check();
        checkParameters(analysisParams, getStudy(), catalogManager, token);

        // Save the somatic samples
        for (String sampleId : analysisParams.getSamples()) {
            // Get sample
            Sample sample = catalogManager.getSampleManager().get(study, sampleId, QueryOptions.empty(), token).first();
            if (sample.isSomatic()) {
                targetSomaticSamples.add(sample);
            }
        }

        // Check custom resources path
        userResourcesPath = checkResourcesDir(analysisParams.getResourcesDir(), getStudy(), catalogManager, token);
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = Arrays.asList(PREPARE_QC_STEP);
        if (CollectionUtils.isEmpty(analysisParams.getSkipAnalysis())) {
            steps.addAll(Arrays.asList(STATS_QC_STEP, SOMATIC_QC_STEP));
        } else {
            if (!analysisParams.getSkipAnalysis().contains(SampleVariantStatsAnalysis.ID)) {
                steps.add(STATS_QC_STEP);
            }
            if (!analysisParams.getSkipAnalysis().contains(SIGNATURE_ANALYSIS_ID)
                    || !analysisParams.getSkipAnalysis().contains(GENOME_PLOT_ANALYSIS_ID)) {
                steps.add(SOMATIC_QC_STEP);
            }
        }
        if (!Boolean.TRUE.equals(analysisParams.getSkipIndex())) {
            steps.add(INDEX_QC_STEP);
        }
        return steps;
    }

    @Override
    protected void run() throws ToolException {
        List<String> steps = getSteps();

        // Main steps
        step(PREPARE_QC_STEP, this::prepareQualityControl);
        if (steps.contains(STATS_QC_STEP)) {
            step(STATS_QC_STEP, this::runSampleVariantStatsStep);
        }
        if (steps.contains(SOMATIC_QC_STEP)) {
            step(SOMATIC_QC_STEP, this::runSomaticStep);
        }
        if (steps.contains(INDEX_QC_STEP)) {
            step(STATS_QC_STEP, this::indexQualityControl);
        }

        // Clean execution
        clean();
    }

    protected void prepareQualityControl() throws ToolException {
        try {
            ObjectWriter objectWriter = JacksonUtils.getDefaultObjectMapper().writerFor(Sample.class);
            for (String sampleId : analysisParams.getSamples()) {
                // Get sample
                Sample sample = catalogManager.getSampleManager().get(study, sampleId, QueryOptions.empty(), token).first();
                if (isSampleIndexed(sample)) {

                    // Add sample to target sample list, i.e., for those samples, QC will be performed
                    samples.add(sample);
                    failedAnalysisPerSample.put(sample.getId(), new HashSet<>());

                    // Only somatic samples need VCF and JSON files to compute signature, genome plot,...
                    if (sample.isSomatic()) {
                        targetSomaticSamples.add(sample);

                        // Export sample variants (VCF format)
                        // Create the query based on whether a trio is present or not
                        Query query = new Query();
                        query.append(VariantQueryParam.SAMPLE.key(), sampleId + ":0/1,1/1")
                                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleId)
                                .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT")
                                .append(VariantQueryParam.STUDY.key(), study)
                                .append(VariantQueryParam.TYPE.key(), VariantType.SNV)
                                .append(VariantQueryParam.REGION.key(), Arrays.asList("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22"
                                        .split(",")));

                        // Create query options
                        QueryOptions queryOptions = new QueryOptions().append(QueryOptions.INCLUDE, "id,studies.samples");

                        // Export to VCF.GZ format
                        String basename = getOutDir().resolve(sampleId).toAbsolutePath().toString();
                        getVariantStorageManager().exportData(basename, VCF_GZ, null, query, queryOptions, token);

                        // Check VCF file
                        Path vcfPath = Paths.get(basename + "." + VCF_GZ.getExtension());
                        if (!Files.exists(vcfPath)) {
                            throw new ToolException("Something wrong happened when exporting VCF file for sample ID '" + sampleId + "'. VCF"
                                    + " file " + vcfPath + " was not created. Export query = " + query.toJson() + "; export query"
                                    + " options = " + queryOptions.toJson());
                        }
                        vcfPaths.add(vcfPath);

                        // Export individual (JSON format)
                        Path jsonPath = Paths.get(basename + "." + JSON.getExtension());
                        objectWriter.writeValue(jsonPath.toFile(), sample);

                        // Check sample JSON file
                        if (!Files.exists(jsonPath)) {
                            throw new ToolException("Something wrong happened when saving JSON file for sample ID '" + sampleId + "'. JSON"
                                    + " file " + jsonPath + " was not created.");
                        }
                        jsonPaths.add(jsonPath);
                    }
                }
            }
        } catch (CatalogException | IOException | StorageEngineException e) {
            // Clean execution
            clean();
            throw new ToolException(e);
        }

        // Prepare resource files
        if (CollectionUtils.isNotEmpty(targetSomaticSamples)) {
            prepareResources();
        }
    }

    protected void runSampleVariantStatsStep() throws ToolException {
        if (CollectionUtils.isEmpty(samples)) {
            addWarning("No samples available for quality control. Please check your input parameters.");
            return;
        }

        boolean callToolRunner;
        ToolRunner toolRunner = new ToolRunner(getOpencgaHome().toAbsolutePath().toString(), getCatalogManager(),
                getVariantStorageManager());

        for (Sample sample : samples) {
            SampleVariantStatsAnalysisParams statsParams = new SampleVariantStatsAnalysisParams()
                    .setSample(Collections.singletonList(sample.getId()))
                    .setIndex(!Boolean.TRUE.equals(analysisParams.getSkipIndex()))
                    .setIndexId(analysisParams.getVsId())
                    .setBatchSize(2)
                    .setIndexOverwrite(analysisParams.getOverwrite())
                    .setVariantQuery(new SampleVariantStatsAnalysisParams.VariantQueryParams(analysisParams.getVsQuery().toQuery()));

            callToolRunner = true;

            // Check the sample variant stats folder, and create
            Path statsOutDir = getOutDir().resolve(sample.getId()).resolve(SampleVariantStatsAnalysis.ID);
            if (!Files.exists(statsOutDir)) {
                try {
                    Files.createDirectories(statsOutDir);
                    if (!Files.exists(statsOutDir)) {
                        failedAnalysisPerSample.get(sample.getId()).add(SampleVariantStatsAnalysis.ID);
                        String msg = "Could not create the sample variant stats folder for sample '" + sample.getId() + "'";
                        addError(new ToolException(msg));
                        logger.error(msg);
                        callToolRunner = false;
                    }
                } catch (IOException e) {
                    failedQcSet.add(sample.getId());
                    failedAnalysisPerSample.get(sample.getId()).add(SampleVariantStatsAnalysis.ID);
                    addError(e);
                    logger.error("Creating sample variant stats folder for sample '" + sample.getId() + "'", e);
                    callToolRunner = false;
                }
            }
            if (callToolRunner) {
                try {
                    toolRunner.execute(SampleVariantStatsAnalysis.class, getStudy(), statsParams, statsOutDir, null, false, getToken());
                } catch (ToolException e) {
                    failedQcSet.add(sample.getId());
                    failedAnalysisPerSample.get(sample.getId()).add(SampleVariantStatsAnalysis.ID);
                    addError(e);
                    logger.error("Error running sample variant stats for sample '" + sample.getId() + "'", e);
                }
            }
        }
    }

    protected void runSomaticStep() throws ToolException {
        if (CollectionUtils.isEmpty(targetSomaticSamples)) {
            addWarning("No somatic samples available for quality control. Please check your input parameters.");
            return;
        }

        // Update the parameter 'samples' by using only the somatic ones before calling the executor
        analysisParams.setSamples(targetSomaticSamples.stream().map(Sample::getId).collect(Collectors.toList()));

        // Get executor to execute Python script that computes the family QC
        SampleVariantQcAnalysisExecutor executor = getToolExecutor(SampleVariantQcAnalysisExecutor.class);
        executor.setVcfPaths(vcfPaths)
                .setJsonPaths(jsonPaths)
                .setQcParams(analysisParams)
                .execute();
    }

    /**
     * Update quality control for each sample by parsing the QC report for signature and genome plot analyses.
     *
     * @throws ToolException Tool exception
     */
    private void indexQualityControl() throws ToolException {
        // Create readers for each QC report
        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        ObjectReader signatureReader = JacksonUtils.getDefaultObjectMapper().readerFor(Signature.class);
        ObjectReader signatureListReader = JacksonUtils.getDefaultObjectMapper().readerFor(new TypeReference<List<Signature>>() {});
        ObjectReader genomePlotReader = JacksonUtils.getDefaultObjectMapper().readerFor(GenomePlot.class);
        ObjectReader genomePlotListReader = JacksonUtils.getDefaultObjectMapper().readerFor(new TypeReference<List<GenomePlot>>() {});

        QualityControlStatus qcStatus;
        SampleQualityControl sampleQc;
        String logMsg;

        for (Sample sample : targetSomaticSamples) {
            sampleQc = new SampleQualityControl();

            // Check and parse the signature results
            Path qcPath = getOutDir().resolve(sample.getId()).resolve(SIGNATURE_ANALYSIS_ID).resolve(QC_RESULTS_FILENAME);
            if (!Files.exists(qcPath)) {
                failedQcSet.add(sample.getId());
                failedAnalysisPerSample.get(sample.getId()).add(SIGNATURE_ANALYSIS_ID);
                sampleQc.getVariant().setSignatures(Collections.emptyList());

                logMsg = FAILURE_FILE + qcPath.getFileName() + NOT_FOUND + getIdLogMessage(sample.getId(), SAMPLE_QC_TYPE);
                addError(new ToolException(logMsg));
                logger.error(logMsg);
            } else {
                try {
                    List<Signature> signatureList = isQcArray(qcPath)
                            ? signatureListReader.readValue(qcPath.toFile())
                            : Collections.singletonList(signatureReader.readValue(qcPath.toFile()));

                    sampleQc.getVariant().setSignatures(signatureList);
                } catch (IOException e) {
                    failedQcSet.add(sample.getId());
                    failedAnalysisPerSample.get(sample.getId()).add(SIGNATURE_ANALYSIS_ID);
                    sampleQc.getVariant().setSignatures(Collections.emptyList());

                    logMsg = FAILURE_ERROR_PARSING_QC_JSON_FILE + qcPath.getFileName() + "'" + getIdLogMessage(sample.getId(),
                            INDIVIDUAL_QC_TYPE);
                    addError(e);
                    logger.error(logMsg, e);
                }
            }

            // Check and parse the genome plot results
            qcPath = getOutDir().resolve(sample.getId()).resolve(GENOME_PLOT_ANALYSIS_ID).resolve(QC_RESULTS_FILENAME);
            if (!Files.exists(qcPath)) {
                failedQcSet.add(sample.getId());
                failedAnalysisPerSample.get(sample.getId()).add(GENOME_PLOT_ANALYSIS_ID);
                sampleQc.getVariant().setGenomePlots(Collections.emptyList());

                logMsg = FAILURE_FILE + qcPath.getFileName() + NOT_FOUND + getIdLogMessage(sample.getId(), SAMPLE_QC_TYPE);
                addError(new ToolException(logMsg));
                logger.error(logMsg);
            } else {
                try {
                    List<GenomePlot> genomePlotList = isQcArray(qcPath)
                            ? genomePlotListReader.readValue(qcPath.toFile())
                            : Collections.singletonList(genomePlotReader.readValue(qcPath.toFile()));

                    sampleQc.getVariant().setGenomePlots(genomePlotList);
                } catch (IOException e) {
                    failedQcSet.add(sample.getId());
                    failedAnalysisPerSample.get(sample.getId()).add(GENOME_PLOT_ANALYSIS_ID);
                    sampleQc.getVariant().setGenomePlots(Collections.emptyList());

                    logMsg = FAILURE_ERROR_PARSING_QC_JSON_FILE + qcPath.getFileName() + "'" + getIdLogMessage(sample.getId(),
                            SAMPLE_QC_TYPE);
                    addError(e);
                    logger.error(logMsg, e);
                }
            }

            if (CollectionUtils.isEmpty(failedAnalysisPerSample.get(sample.getId()))) {
                qcStatus = new QualityControlStatus(READY, SUCCESS);
            } else {
                qcStatus = new QualityControlStatus(ERROR, "Failed analysis: "
                        + StringUtils.join(failedAnalysisPerSample.get(sample.getId()), ","));
            }

            try {
                // Update catalog: quality control and status
                SampleUpdateParams updateParams = new SampleUpdateParams()
                        .setQualityControl(sampleQc)
                        .setQualityControlStatus(qcStatus);
                catalogManager.getSampleManager().update(getStudy(), sample.getId(), updateParams, null, token);
            } catch (CatalogException e) {
                failedQcSet.add(sample.getId());
                logMsg = FAILURE_COULD_NOT_UPDATE_QUALITY_CONTROL_IN_OPEN_CGA_CATALOG + getIdLogMessage(sample.getId(),
                        INDIVIDUAL_QC_TYPE);
                addError(e);
                logger.error(logMsg, e);
            }
        }

        checkFailedQcCounter(samples.size(), SAMPLE_QC_TYPE);
    }

    /**
     * Check sample QC parameters before submitting the QC job, including catalog permissions
     * IMPORTANT: it is a static method to be called by the WSServer before submitting the QC job.
     *
     * @param params         Sample QC parameters
     * @param studyId        Study ID
     * @param catalogManager Catalog manager
     * @param token          Token to access OpenCGA catalog
     * @throws ToolException Tool exception
     */
    public static void checkParameters(SampleQcAnalysisParams params, String studyId, CatalogManager catalogManager, String token)
            throws ToolException {
        // Check study
        checkStudy(studyId, catalogManager, token);

        // Check permissions
        checkPermissions(WRITE_SAMPLES, params.getSkipIndex(), studyId, catalogManager, token);

        // Check samples in catalog
        if (CollectionUtils.isEmpty(params.getSamples())) {
            throw new ToolException("Missing samples");
        }

        // Collect possible errors in a map where key is the sample ID and value is the error
        int indexCount = 0;
        Map<String, String> errors = new HashMap<>();
        for (String sampleId : params.getSamples()) {
            // Get sample from catalog
            try {
                OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(studyId, sampleId, QueryOptions.empty(), token);
                if (sampleResult.getNumResults() == 0) {
                    errors.put(sampleId, "Sample not found");
                } else if (sampleResult.getNumResults() > 1) {
                    errors.put(sampleId, "More than one sample found");
                } else {
                    if (isSampleIndexed(sampleResult.first())) {
                        indexCount++;
                    }
                }
            } catch (CatalogException e) {
                errors.put(sampleId, Arrays.toString(e.getStackTrace()));
            }
        }

        // Check error
        if (MapUtils.isNotEmpty(errors)) {
            throw new ToolException("Found the following error for sample IDs: " + StringUtils.join(errors.entrySet().stream().map(
                    e -> "Sample ID " + e.getKey() + ": " + e.getValue()).collect(Collectors.toList()), ","));
        }

        // Check indexed samples
        if (indexCount == 0) {
            throw new ToolException("None of the input samples are indexed");
        }

        // Check resources dir
        checkResourcesDir(params.getResourcesDir(), studyId, catalogManager, token);
    }
}