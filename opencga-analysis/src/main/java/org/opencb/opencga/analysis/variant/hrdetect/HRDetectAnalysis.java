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

package org.opencb.opencga.analysis.variant.hrdetect;

import com.mongodb.client.ListCollectionsIterable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.HRDetect;
import org.opencb.biodata.models.clinical.qc.Signature;
import org.opencb.biodata.models.clinical.qc.SignatureFitting;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.sample.SampleVariantQualityControlMetrics;
import org.opencb.opencga.core.models.variant.HRDetectAnalysisParams;
import org.opencb.opencga.core.models.variant.MutationalSignatureAnalysisParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.HRDetectAnalysisExecutor;
import org.opencb.opencga.core.tools.variant.MutationalSignatureAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

@Tool(id = HRDetectAnalysis.ID, resource = Enums.Resource.VARIANT)
public class HRDetectAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "hr-detect";
    public static final String DESCRIPTION = "Run HRDetect analysis for a given somatic sample.";

    public final static String HRDETECT_SCORES_FILENAME_DEFAULT = "data_matrix.tsv";

    @ToolParams
    private HRDetectAnalysisParams hrdetectParams = new HRDetectAnalysisParams();

    private Sample somaticSample;
    private Sample germlineSample;
    private String assembly;
    private Path pathSnvFittingRData;
    private Path pathSvFittingRData;
    private ObjectMap cnvQuery;
    private ObjectMap indelQuery;

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        assembly = ResourceUtils.getAssembly(catalogManager, study, token);
        if (StringUtils.isEmpty(assembly)) {
            throw new ToolException("Missing assembly for study '" + study + "'");
        }

        if (StringUtils.isEmpty(hrdetectParams.getSampleId())) {
            throw new ToolException("Missing sample ID");
        }

        if (StringUtils.isEmpty(hrdetectParams.getSnvFittingId())) {
            throw new ToolException("Missing mutational signature fitting ID for SNV");
        }

        if (StringUtils.isEmpty(hrdetectParams.getSvFittingId())) {
            throw new ToolException("Missing mutational signature fitting ID for SV");
        }

        if (StringUtils.isEmpty(hrdetectParams.getCnvQuery())) {
            throw new ToolException("Missing CNV query");
        }

        if (StringUtils.isEmpty(hrdetectParams.getIndelQuery())) {
            throw new ToolException("Missing INDEL query");
        }

        // Check sample
        somaticSample = checkSample(hrdetectParams.getSampleId());
        if (!somaticSample.isSomatic()) {
            throw new ToolException("Mismatch sample from CNV query '" + somaticSample.getId() + "' must be somatic");
        }
        checkSampleQualityControl(somaticSample);

        SignatureFitting snvFitting = null;
        SignatureFitting svFitting = null;
        List<Signature> signatures = somaticSample.getQualityControl().getVariant().getSignatures();
        for (Signature signature : signatures) {
            if (CollectionUtils.isNotEmpty(signature.getFittings())) {
                for (SignatureFitting fitting : signature.getFittings()) {
                    if (hrdetectParams.getSnvFittingId().equals(fitting.getId()) && snvFitting == null) {
                        // Take the first SNV fitting matching ID
                        snvFitting = fitting;
                    } else if (hrdetectParams.getSvFittingId().equals(fitting.getId()) && svFitting == null) {
                        // Take the first SV fitting matching ID
                        svFitting = fitting;
                    }
                }
            }
        }

        if (snvFitting == null) {
            throw new ToolException("Unable to compute HRDetect analysis. No SNV fitting with ID '" + hrdetectParams.getSnvFittingId()
                    + "' found for sample '" + hrdetectParams.getSampleId() + "'");
        }
        if (svFitting == null) {
            throw new ToolException("Unable to compute HRDetect analysis. No SV fitting with ID '" + hrdetectParams.getSvFittingId()
                    + "' found for sample '" + hrdetectParams.getSampleId() + "'");
        }

        pathSnvFittingRData = getFittingRDataFile(snvFitting.getFiles());
        if (!pathSnvFittingRData.toFile().exists()) {
            throw new ToolException("Unable to compute HRDetect analysis. No .rData file found for SNV fitting with ID '"
                    + hrdetectParams.getSvFittingId() + "' for sample '" + hrdetectParams.getSampleId() + "'");
        }

        pathSvFittingRData = getFittingRDataFile(svFitting.getFiles());
        if (!pathSvFittingRData.toFile().exists()) {
            throw new ToolException("Unable to compute HRDetect analysis. No .rData file found for SV fitting with ID '"
                    + hrdetectParams.getSvFittingId() + "' for sample '" + hrdetectParams.getSampleId() + "'");
        }

        // Check CNV query
        cnvQuery = JacksonUtils.getDefaultObjectMapper().readValue(hrdetectParams.getCnvQuery(), ObjectMap.class);
        Individual individual = IndividualQcUtils.getIndividualBySampleId(getStudy(), hrdetectParams.getSampleId(), getCatalogManager(),
                getToken());
        if (individual == null) {
            throw new ToolException("Unable to compute HRDetect analysis. No individual found for sample '"
                    + hrdetectParams.getSampleId() + "', that individual must have at least two samples: somatic and germline");
        }
        List<Sample> samples = individual.getSamples();
        if (samples.size() < 2) {
            throw new ToolException("For CNV query processing, individual (" + individual.getId() + ") must have at least two"
                    + " samples: somatic and germline");
        }
        for (Sample sample : samples) {
            if (!sample.isSomatic()) {
                germlineSample = sample;
                break;
            }
        }
        if (germlineSample == null) {
            throw new ToolException("Germline sample not found for individual '" + individual.getId() + "', it is mandatory for CNV query"
                    + " processing");
        }

        // Check INDEL query
        indelQuery = JacksonUtils.getDefaultObjectMapper().readValue(hrdetectParams.getIndelQuery(), ObjectMap.class);
        if (!indelQuery.containsKey(VariantQueryParam.SAMPLE.key())) {
            logger.info("Setting sample in INDEL query");
            indelQuery.put(VariantQueryParam.SAMPLE.key(), somaticSample.getId());
        }
        if (!somaticSample.getId().equals(indelQuery.getString(VariantQueryParam.SAMPLE.key()))) {
            throw new ToolException("Mismatch sample from INDEL query '" + cnvQuery.getString(VariantQueryParam.SAMPLE.key())+ "' and"
                    + " sample '" + somaticSample.getId() + "'");
        }

        // Log messages
        logger.info("HRDetect ID: {}", hrdetectParams.getId());
        logger.info("Study: {}", study);
        logger.info("Assembly: {}", assembly);
        logger.info("Somatatic sample ID: {}", somaticSample.getId());
        logger.info("Germline sample ID: {}", germlineSample.getId());
        logger.info("Signature fitting ID for SNV: {}", hrdetectParams.getSnvFittingId());
        logger.info("Signature fitting ID for SV: {}", hrdetectParams.getSvFittingId());
        logger.info("CNV query: {}", cnvQuery.toJson());
        logger.info("INDEL query: {}", indelQuery.toJson());
        logger.info("y (SNV3): {}", hrdetectParams.getSnv3CustomName());
        logger.info("z (SNV8): {}", hrdetectParams.getSnv8CustomName());
        logger.info("Y (SV3): {}", hrdetectParams.getSv3CustomName());
        logger.info("Z (SV8): {}", hrdetectParams.getSv8CustomName());
        logger.info("Bootstrap: {}", hrdetectParams.isBootstrap());
    }

    @Override
    protected void run() throws ToolException {
        step(getId(), () -> {
            HRDetectAnalysisExecutor toolExecutor = getToolExecutor(HRDetectAnalysisExecutor.class);

            toolExecutor.setStudy(study)
                    .setSomaticSample(somaticSample.getId())
                    .setGermlineSample(germlineSample.getId())
                    .setAssembly(assembly)
                    .setSnvRDataPath(pathSnvFittingRData)
                    .setSvRDataPath(pathSvFittingRData)
                    .setCnvQuery(cnvQuery)
                    .setIndelQuery(indelQuery)
                    .setSnv3CustomName(hrdetectParams.getSnv3CustomName())
                    .setSnv8CustomName(hrdetectParams.getSnv8CustomName())
                    .setSv3CustomName(hrdetectParams.getSv3CustomName())
                    .setSv8CustomName(hrdetectParams.getSv8CustomName())
                    .setBootstrap(hrdetectParams.isBootstrap())
                    .execute();

            // Parse results and update quality control for the catalog sample
            Sample sample = checkSample(hrdetectParams.getSampleId());
            HRDetect hrDetect = parseResult(getOutDir());
            SampleQualityControl qc = sample.getQualityControl();
            if (qc == null) {
                qc = new SampleQualityControl();
            }
            if (qc.getVariant() == null) {
                qc.setVariant(new SampleVariantQualityControlMetrics());
            }
            if (qc.getVariant().getHrDetects() == null) {
                qc.getVariant().setHrDetects(new ArrayList<>());
            }
            qc.getVariant().getHrDetects().add(hrDetect);
            catalogManager.getSampleManager().update(getStudy(), sample.getId(), new SampleUpdateParams().setQualityControl(qc),
                    QueryOptions.empty(), getToken());
        });
    }

    public HRDetect parseResult(Path dir) throws IOException {
        HRDetect result = new HRDetect()
                .setId(hrdetectParams.getId())
                .setDescription(hrdetectParams.getDescription())
                .setSnvFittingId(hrdetectParams.getSnvFittingId())
                .setSvFittingId(hrdetectParams.getSvFittingId())
//                .setCnvQuery(JacksonUtils.getDefaultObjectMapper().readValue(hrdetectParams.getCnvQuery(), ObjectMap.class))
//                .setIndelQuery(JacksonUtils.getDefaultObjectMapper().readValue(hrdetectParams.getIndelQuery(), ObjectMap.class));
                ;

        // Set other params
        ObjectMap params = new ObjectMap();
        if (StringUtils.isNotEmpty(hrdetectParams.getSnv3CustomName())) {
            params.append("snv3CustomName", hrdetectParams.getSnv3CustomName());
        }
        if (StringUtils.isNotEmpty(hrdetectParams.getSnv8CustomName())) {
            params.append("snv8CustomName", hrdetectParams.getSnv8CustomName());
        }
        if (StringUtils.isNotEmpty(hrdetectParams.getSv3CustomName())) {
            params.append("sv3CustomName", hrdetectParams.getSv3CustomName());
        }
        if (StringUtils.isNotEmpty(hrdetectParams.getSv8CustomName())) {
            params.append("sv8CustomName", hrdetectParams.getSv8CustomName());
        }
        if (params.size() > 0) {
            result.setParams(params);
        }

        // Read scores
        ObjectMap scores = new ObjectMap();
        File scoresFile = dir.resolve(HRDETECT_SCORES_FILENAME_DEFAULT).toFile();
        if (scoresFile.exists()) {
            List<String> lines = FileUtils.readLines(scoresFile, Charset.defaultCharset());
            if (lines.size() > 1) {
                String[] labels = lines.get(0).split("\t");
                String[] values = lines.get(1).split("\t");
                for (int i = 0; i < labels.length; i++) {
                    try {
                        scores.put(labels[i], Float.parseFloat(values[i + 1]));
                    } catch (NumberFormatException e) {
                        scores.put(labels[i], Float.NaN);
                    }
                }
            }
        }
        if (MapUtils.isNotEmpty(scores)) {
            result.setScores(scores);
        }

        // TODO: files to be added ?

        return result;
    }

    private Sample checkSample(String sampleId) throws ToolException, CatalogException {
        study = catalogManager.getStudyManager().get(study, QueryOptions.empty(), token).first().getFqn();
        OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(study, sampleId, QueryOptions.empty(), token);
        if (sampleResult.getNumResults() != 1) {
            throw new ToolException("Unable to compute HRDetect analysis. Sample '" + hrdetectParams.getSampleId() + "' not found");
        }

        return sampleResult.first();
    }

    private void checkSampleQualityControl(Sample sample) throws ToolException {
        if (sample.isSomatic()) {
            // Check signatures are present in the quality control (only for somatic sample)
            if (sample.getQualityControl() == null || sample.getQualityControl().getVariant() == null ||
                    CollectionUtils.isEmpty(sample.getQualityControl().getVariant().getSignatures())) {
                throw new ToolException("Unable to compute HRDetect analysis. No mutational signatures found for sample '"
                        + hrdetectParams.getSampleId() + "'");
            }
        }
    }

    private Path getFittingRDataFile(List<String> files) {
        if (CollectionUtils.isEmpty(files)) {
            return null;
        }
        for (String file : files) {
            if (file.endsWith("rData")) {
                return getOutDir().getParent().resolve(file);
            }
        }
        return null;
    }
}

