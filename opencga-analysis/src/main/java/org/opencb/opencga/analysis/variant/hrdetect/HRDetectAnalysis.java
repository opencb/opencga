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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.Signature;
import org.opencb.biodata.models.clinical.qc.SignatureFitting;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
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
import java.util.List;

@Tool(id = HRDetectAnalysis.ID, resource = Enums.Resource.VARIANT)
public class HRDetectAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "hrdetect";
    public static final String DESCRIPTION = "Run HRDetect analysis for a given sample.";

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

        Path pathSnvFittingRData = getFittingRDataFile(snvFitting.getFiles());
        if (!pathSnvFittingRData.toFile().exists()) {
            throw new ToolException("Unable to compute HRDetect analysis. No .rData file found for SNV fitting with ID '"
                    + hrdetectParams.getSvFittingId() + "' for sample '" + hrdetectParams.getSampleId() + "'");
        }

        Path pathSvFittingRData = getFittingRDataFile(svFitting.getFiles());
        if (!pathSvFittingRData.toFile().exists()) {
            throw new ToolException("Unable to compute HRDetect analysis. No .rData file found for SV fitting with ID '"
                    + hrdetectParams.getSvFittingId() + "' for sample '" + hrdetectParams.getSampleId() + "'");
        }

        // Check CNV query
        cnvQuery = JacksonUtils.getDefaultObjectMapper().readValue(hrdetectParams.getCnvQuery(), ObjectMap.class);
        if (!cnvQuery.containsKey(VariantQueryParam.SAMPLE.key())) {
            throw new ToolException("Unable to compute HRDetect analysis. No samples found in CNV query. It must contain somatic and"
                    + " germline sample IDs");
        } else {
            List<String> samples = cnvQuery.getAsStringList(VariantQueryParam.SAMPLE.key());
            if (samples.size() != 2) {
                throw new ToolException("CNV query must contain two samples: somatic and germline ('"
                        + cnvQuery.getString(VariantQueryParam.SAMPLE.key()));
            }
            if (samples.get(0).equals(hrdetectParams.getSampleId())) {
                germlineSample = checkSample(samples.get(1));
            } else if (samples.get(0).equals(hrdetectParams.getSampleId())) {
                germlineSample = checkSample(samples.get(0));
            } else {
                throw new ToolException("Mismatch sample from CNV query '" + cnvQuery.getString(VariantQueryParam.SAMPLE.key())+ "' and"
                        + " sample '" + hrdetectParams.getSampleId() + "'");
            }
            if (germlineSample.isSomatic()) {
                throw new ToolException("Mismatch sample from CNV query '" + germlineSample.getId() + "' must be germline");
            }
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
        logger.info(">>>> Params:");
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
//            toolExecutor.parseResult(getOutDir())
            SampleQualityControl qc = sample.getQualityControl();
            catalogManager.getSampleManager().update(getStudy(), sample.getId(), new SampleUpdateParams().setQualityControl(qc),
                    QueryOptions.empty(), getToken());
        });
    }

//    public Signature parse(Path dir) throws IOException {
//        Signature result = new Signature(signatureParams.getId(), signatureParams.getDescription(), query, "SNV", null, null, null);
//
//        // Context counts
//        File contextFile = dir.resolve(CATALOGUES_FILENAME_DEFAULT).toFile();
//        if (contextFile.exists()) {
//            List<String> lines = FileUtils.readLines(contextFile, Charset.defaultCharset());
//            List<Signature.GenomeContextCount> sigCounts = new ArrayList<>(lines.size() - 1);
//            for (int i = 1; i < lines.size(); i++) {
//                String[] fields = lines.get(i).split("\t");
//                sigCounts.add(new Signature.GenomeContextCount(fields[0], Math.round(Float.parseFloat((fields[1])))));
//            }
//            result.setCounts(sigCounts);
//        }
//
//        // Signature fitting
//        File coeffsFile = dir.resolve(SIGNATURE_COEFFS_FILENAME).toFile();
//        if (coeffsFile.exists()) {
//            SignatureFitting fitting = new SignatureFitting()
//                    .setMethod(signatureParams.getFitMethod())
//                    .setSignatureVersion(signatureParams.getSigVersion());
//
//            // Set source from fit method
//            if (StringUtils.isNotEmpty(getSignatureParams().getSigVersion())) {
//                if (getSignatureParams().getSigVersion().startsWith("COSMIC")) {
//                    fitting.setSignatureSource("COSMIC");
//                } else if (getSignatureParams().getSigVersion().startsWith("RefSig")) {
//                    fitting.setSignatureSource("RefSig");
//                }
//            }
//
//            // Set fitting scores
//            List<String> lines = FileUtils.readLines(coeffsFile, Charset.defaultCharset());
//            String[] labels = lines.get(0).split("\t");
//            String[] values = lines.get(1).split("\t");
//            List<SignatureFitting.Score> scores = new ArrayList<>(labels.length);
//            for (int i = 0; i < labels.length; i++) {
//                String label = labels[i];
//                if (label.contains("_")) {
//                    String[] splits = label.split("_");
//                    label = splits[splits.length - 1];
//                }
//                scores.add(new SignatureFitting.Score(label, Double.parseDouble(values[i + 1])));
//            }
//            fitting.setScores(scores);
//
//            // Set files
//            List<String> files = new ArrayList<>();
//            for (File file : getOutDir().toFile().listFiles()) {
//                if (file.getName().endsWith("pdf")) {
//                    files.add(file.getName());
//                } else if (file.isDirectory()) {
//                    for (File file2 : file.listFiles()) {
//                        if (file2.getName().endsWith("pdf")) {
//                            files.add(file.getName() + "/" + file2.getName());
//                        }
//                    }
//                }
//            }
//            fitting.setFiles(files);
//
//            // Set params
//            ObjectMap params = new ObjectMap();
//            if (signatureParams.getnBoot() != null) {
//                params.append("nBoot", signatureParams.getnBoot());
//            }
//            if (StringUtils.isNotEmpty(signatureParams.getOrgan())) {
//                params.append("organ", signatureParams.getOrgan());
//            }
//            if (signatureParams.getThresholdPerc() != null) {
//                params.append("thresholdPerc", signatureParams.getThresholdPerc());
//            }
//            if (signatureParams.getThresholdPval() != null) {
//                params.append("thresholdPval", signatureParams.getThresholdPval());
//            }
//            if (signatureParams.getMaxRareSigs() != null) {
//                params.append("maxRareSigs", signatureParams.getMaxRareSigs());
//            }
//            if (params.size() > 0) {
//                fitting.setParams(params);
//            }
//            fitting.setParams(params);
//
//            // Set fitting signature
//            result.setFitting(fitting);
//        }
//
//        return result;
//    }

    private Sample checkSample(String sampleId) throws ToolException, CatalogException {
        study = catalogManager.getStudyManager().get(study, QueryOptions.empty(), token).first().getFqn();
        OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(study, sampleId, QueryOptions.empty(), token);
        if (sampleResult.getNumResults() != 1) {
            throw new ToolException("Unable to compute HRDetect analysis. Sample '" + hrdetectParams.getSampleId() + "' not found");
        }

        Sample sample = sampleResult.first();
        if (sample.isSomatic()) {
            // Check signatures are present in the quality control (only for somatic sample)
            if (sample.getQualityControl() == null || sample.getQualityControl().getVariant() == null ||
                    CollectionUtils.isEmpty(sample.getQualityControl().getVariant().getSignatures())) {
                throw new ToolException("Unable to compute HRDetect analysis. No mutational signatures found for sample '"
                        + hrdetectParams.getSampleId() + "'");
            }
        }

        return sample;
    }

    private Path getFittingRDataFile(List<String> files) {
        if (CollectionUtils.isEmpty(files)) {
            return null;
        }
        for (String file : files) {
            if (file.endsWith("rData")) {
                return getOutDir().resolve(file);
            }
        }
        return null;
    }
}

