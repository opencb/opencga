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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.GenomePlot;
import org.opencb.biodata.models.clinical.qc.HRDetect;
import org.opencb.biodata.models.clinical.qc.Signature;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.qc.VariantQcAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.individual.IndividualQualityControlStatus;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.models.sample.SampleQualityControlStatus;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.variant.SampleQcAnalysisParams;
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

import static org.opencb.opencga.core.models.common.QualityControlStatus.COMPUTING;
import static org.opencb.opencga.core.models.sample.SampleQualityControlStatus.*;
import static org.opencb.opencga.core.models.study.StudyPermissions.Permissions.WRITE_SAMPLES;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.JSON;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.VCF_GZ;

@Tool(id = SampleVariantQcAnalysis.ID, resource = Enums.Resource.SAMPLE, description = SampleVariantQcAnalysis.DESCRIPTION)
public class SampleVariantQcAnalysis extends VariantQcAnalysis {

    public static final String ID = "sample-variant-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given sample. It includes variant stats, and if the sample " +
            "is somatic, mutational signature and genome plot are calculated.";

    // Set of somatic samples
    private Set<Sample> somaticSamples = new HashSet<>();

    @ToolParams
    protected final SampleQcAnalysisParams analysisParams = new SampleQcAnalysisParams();

    @Override
    protected void check() throws Exception {
        super.check();
        checkParameters(analysisParams, getStudy(), catalogManager, token);

        // Check for the presence of trios to compute relatedness, and then prepare relatedness resource files
        for (String sampleId : analysisParams.getSamples()) {
            // Get sample
            Sample sample = catalogManager.getSampleManager().get(study, sampleId, QueryOptions.empty(), token).first();
            if (sample.isSomatic()) {
                somaticSamples.add(sample);
            }
        }
        if (CollectionUtils.isNotEmpty(somaticSamples)) {
            // Prepare resource files for somatic samples
            //prepareSignatureResources(analysisParams.getResourcesDir());
            //prepareGenomePlotResources(analysisParams.getResourcesDir());
        }
    }

    @Override
    protected void run() throws ToolException {
        setUpStorageEngineExecutor(study);

        List<Sample> samples = new ArrayList<>();
        LinkedList<Path> sampleVcfPaths = new LinkedList<>();
        LinkedList<Path> sampleJsonPaths = new LinkedList<>();

        try {
            ObjectWriter objectWriter = JacksonUtils.getDefaultObjectMapper().writerFor(Sample.class);
            for (String sampleId : analysisParams.getSamples()) {
                // Get sample
                Sample sample = catalogManager.getSampleManager().get(study, sampleId, QueryOptions.empty(), token).first();

                // Decide if quality control has to be performed
                //   - by checking the quality control status, if it is READY means it has been computed previously, and
                //   - by checking the flag overwrite
                if (sample.getInternal() == null || performQualityControl(sample.getInternal().getQualityControlStatus(),
                        analysisParams.getOverwrite())) {

                    // Set quality control status to COMPUTING to prevent multiple individual QCs from running simultaneously
                    // for the same individual
                    IndividualQualityControlStatus qcStatus = new IndividualQualityControlStatus(COMPUTING,
                            "Performing " + SAMPLE_QC_TYPE + " QC");
                    if (!setQualityControlStatus(qcStatus, sample.getId(), SAMPLE_QC_TYPE)) {
                        continue;
                    }

                    // Create directory to save variants and sample files
                    Path sampleOutPath = Files.createDirectories(getOutDir().resolve(sampleId));
                    if (!Files.exists(sampleOutPath)) {
                        throw new ToolException("Error creating directory: " + sampleOutPath);
                    }

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
                    String basename = sampleOutPath.resolve(sampleId).toAbsolutePath().toString();
                    getVariantStorageManager().exportData(basename, VCF_GZ, null, query, queryOptions, token);

                    // Check VCF file
                    Path sampleVcfPath = Paths.get(basename + "." + VCF_GZ.getExtension());
                    if (!Files.exists(sampleVcfPath)) {
                        throw new ToolException("Something wrong happened when exporting VCF file for sample ID '" + sampleId + "'. VCF"
                                + " file " + sampleVcfPath + " was not created. Export query = " + query.toJson() + "; export query"
                                + " options = " + queryOptions.toJson());
                    }
                    sampleVcfPaths.add(sampleVcfPath);

                    // Export individual (JSON format)
                    Path sampleJsonPath = Paths.get(basename + "." + JSON.getExtension());
                    objectWriter.writeValue(sampleJsonPath.toFile(), sample);

                    // Check sample JSON file
                    if (!Files.exists(sampleJsonPath)) {
                        throw new ToolException("Something wrong happened when saving JSON file for sample ID '" + sampleId + "'. JSON"
                                + " file " + sampleJsonPath + " was not created.");
                    }
                    sampleJsonPaths.add(sampleJsonPath);

                    // Add sample to the list
                    samples.add(sample);
                }
            }
        } catch (CatalogException | IOException | StorageEngineException e) {
            throw new ToolException(e);
        }

        // Get executor to execute Python script that computes the family QC
        SampleVariantQcAnalysisExecutor executor = getToolExecutor(SampleVariantQcAnalysisExecutor.class);
        executor.setVcfPaths(sampleVcfPaths)
                .setJsonPaths(sampleJsonPaths)
                .setQcParams(analysisParams);

        // Step by step
        step(executor::execute);

        // Parse Python script results
        if (!Boolean.TRUE.equals(analysisParams.getSkipIndex())) {
            updateSampleQualityControl(samples);
        }
    }

    /**
     * Update quality control for each sample by parsing the QC report for signature and genome plot analyses.
     *
     * @param samples List of samples
     * @throws ToolException Tool exception
     */
    private void updateSampleQualityControl(List<Sample> samples) throws ToolException {
        // Create readers for each QC report
        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        ObjectReader signatureReader = JacksonUtils.getDefaultObjectMapper().readerFor(Signature.class);
        ObjectReader hrDetectReader = JacksonUtils.getDefaultObjectMapper().readerFor(HRDetect.class);
        ObjectReader genomePlotReader = JacksonUtils.getDefaultObjectMapper().readerFor(GenomePlot.class);

        for (Sample sample : samples) {
            // Check output files
            Path qcPath = getOutDir().resolve(sample.getId());
            if (!Files.exists(qcPath)) {
                String msg = "Quality control error for sample " + sample.getId() + ": folder " + qcPath + " not found."
                        + " None quality control was performed.";
                logger.error(msg);
                addError(new ToolException(msg));
                continue;
            }

            int qcCode = NONE_READY;
            SampleQualityControl sampleQc = sample.getQualityControl();

            // Check signature
            Signature signature = checkQcReport(sample.getId(), SIGNATURE_ANALYSIS_ID, analysisParams.getSkip(), qcPath, SAMPLE_QC_TYPE,
                    signatureReader);
            if (signature != null) {
                qcCode |= SIGNATURE_READY;
                sampleQc.getVariant().getSignatures().add(signature);
            }

            // Check hr detect
            HRDetect hrDetect = checkQcReport(sample.getId(), HR_DETECT_ANALYSIS_ID, analysisParams.getSkip(), qcPath, SAMPLE_QC_TYPE,
                    hrDetectReader);
            if (hrDetect != null) {
                qcCode |= HR_DETECT_READY;
                sampleQc.getVariant().getHrDetects().add(hrDetect);
            }

            // Check genome plot
            GenomePlot genomePlot = checkQcReport(sample.getId(), GENOME_PLOT_ANALYSIS_ID, analysisParams.getSkip(), qcPath, SAMPLE_QC_TYPE,
                    genomePlotReader);
            if (genomePlot != null) {
                qcCode |= GENOME_PLOT_READY;
                sampleQc.getVariant().getGenomePlots().add(genomePlot);
            }

            // Update catalog (quality control and status) if necessary
            if (qcCode != NONE_READY) {
                // Update the sample QC code with the current one
                SampleQualityControlStatus qcStatus = new SampleQualityControlStatus(
                        qcCode | sample.getInternal().getQualityControlStatus().getCode(), "");
                try {
                    SampleUpdateParams updateParams = new SampleUpdateParams()
                            .setQualityControl(sampleQc)
                            .setQualityControlStatus(qcStatus);
                    catalogManager.getSampleManager().update(getStudy(), sample.getId(), updateParams, null, token);
                } catch (CatalogException e) {
                    logger.error("Could not update quality control in OpenCGA catalog for sample " + sample.getId(), e);
                    addError(e);
                }
            }
        }
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
        checkPermissions(WRITE_SAMPLES, studyId, catalogManager, token);

        // Check samples in catalog
        // Collect possible errors in a map where key is the sample ID and value is the error
        Map<String, String> errors = new HashMap<>();
        for (String sampleId : params.getSamples()) {
            // Get sample from catalog
            Sample sample;
            try {
                OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(studyId, sampleId, QueryOptions.empty(), token);
                if (sampleResult.getNumResults() == 0) {
                    errors.put(sampleId, "Sample not found");
                } else if (sampleResult.getNumResults() > 1) {
                    errors.put(sampleId, "More than one sample found");
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

        // Check resources dir
        checkResourcesDir(params.getResourcesDir(), studyId, catalogManager, token);
    }
}