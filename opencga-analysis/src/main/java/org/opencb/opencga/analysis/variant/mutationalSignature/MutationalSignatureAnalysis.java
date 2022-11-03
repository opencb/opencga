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

package org.opencb.opencga.analysis.variant.mutationalSignature;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.Signature;
import org.opencb.biodata.models.clinical.qc.SignatureFitting;
import org.opencb.biodata.models.clinical.qc.SignatureFittingScore;
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
import org.opencb.opencga.core.models.variant.MutationalSignatureAnalysisParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.MutationalSignatureAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Tool(id = MutationalSignatureAnalysis.ID, resource = Enums.Resource.VARIANT)
public class MutationalSignatureAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "mutational-signature";
    public static final String DESCRIPTION = "Run mutational signature analysis for a given sample.";

    public final static String SIGNATURE_COEFFS_FILENAME = "exposures.tsv";
    public final static String SIGNATURE_FITTING_FILENAME = "signature_summary.png";
    public final static String CATALOGUES_FILENAME_DEFAULT = "catalogues.tsv";


    public final static String QC_UPDATE_KEYNAME = "qcUpdate";

    @ToolParams
    private MutationalSignatureAnalysisParams signatureParams = new MutationalSignatureAnalysisParams();

    private String sample;
    private String assembly;
    private ObjectMap query;
    private String catalogues;
    private String signaturesFile;
    private String rareSignaturesFile;

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        // Fitting from sample/query
        if (signatureParams.getQuery() == null) {
            throw new ToolException("Missing signature query");
        }
        query = JacksonUtils.getDefaultObjectMapper().readValue(signatureParams.getQuery(), ObjectMap.class);
        logger.info("Signagture query: {}", signatureParams.getQuery());
        if (!query.containsKey(VariantQueryParam.SAMPLE.key())) {
            throw new ToolException("Missing sample in the signature query");
        }
        if (StringUtils.isEmpty(query.getString(VariantQueryParam.SAMPLE.key()))) {
            throw new ToolException("Sample is empty in the signature query");
        }

        // Get sample
        sample = query.getString(VariantQueryParam.SAMPLE.key());
        if (sample.contains(":")) {
            sample = sample.split(":")[0];
        }

        // Get assembly
        assembly = ResourceUtils.getAssembly(catalogManager, study, token);
        if (StringUtils.isEmpty(assembly)) {
            throw new ToolException("Missing assembly for study '" + study + "'");
        }
        // TODO: improve this
        switch (assembly.toUpperCase()) {
            case "GRCH37":
                assembly = "GRCh37";
                break;
            case "GRCH38":
                assembly = "GRCh38";
                break;
            default:
                break;
        }

        try {
            // Check sample
            study = catalogManager.getStudyManager().get(study, QueryOptions.empty(), token).first().getFqn();
            OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(study, sample, QueryOptions.empty(), token);
            if (sampleResult.getNumResults() != 1) {
                throw new ToolException("Unable to compute mutational signature analysis. Sample '" + sample + "' not found");
            }

            // Check signatures file
            if (StringUtils.isNotEmpty(signatureParams.getFitSignaturesFile())) {
                org.opencb.opencga.core.models.file.File catalogFile = AnalysisUtils.getCatalogFile(signatureParams.getFitSignaturesFile(),
                        getStudy(), catalogManager.getFileManager(), getToken());
                signaturesFile = catalogFile.getUri().getPath();
            }

            // Check rare signatures file
            if (StringUtils.isNotEmpty(signatureParams.getFitRareSignaturesFile())) {
                org.opencb.opencga.core.models.file.File catalogFile = AnalysisUtils.getCatalogFile(
                        signatureParams.getFitRareSignaturesFile(), getStudy(), catalogManager.getFileManager(), getToken());
                rareSignaturesFile = catalogFile.getUri().getPath();
            }
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Log messages
        logger.info("Signagture fit id: {}", signatureParams.getFitId());
        logger.info("Signagture fit method: {}", signatureParams.getFitMethod());
        logger.info("Signagture fit sig. version: {}", signatureParams.getFitSigVersion());
        logger.info("Signagture fit organ: {}", signatureParams.getFitOrgan());
        logger.info("Signagture fit n boot: {}", signatureParams.getFitNBoot());
        logger.info("Signagture fit threshold percentage: {}", signatureParams.getFitThresholdPerc());
        logger.info("Signagture fit threshold p-value: {}", signatureParams.getFitThresholdPval());
        logger.info("Signagture fit max. rare sigs.: {}", signatureParams.getFitMaxRareSigs());
        logger.info("Signagture fit signatures file: {}", signaturesFile);
        logger.info("Signagture fit rare signatures file: {}", rareSignaturesFile);
    }

    @Override
    protected void run() throws ToolException {
        step(getId(), () -> {
            MutationalSignatureAnalysisExecutor toolExecutor = getToolExecutor(MutationalSignatureAnalysisExecutor.class);

            toolExecutor.setStudy(study)
                    .setSample(sample)
                    .setAssembly(assembly)
                    .setQueryId(signatureParams.getId())
                    .setQueryDescription(signatureParams.getDescription())
                    .setQuery(query)
                    .setCatalogues(catalogues)
                    .setFitMethod(signatureParams.getFitMethod())
                    .setSigVersion(signatureParams.getFitSigVersion())
                    .setOrgan(signatureParams.getFitOrgan())
                    .setnBoot(signatureParams.getFitNBoot())
                    .setThresholdPerc(signatureParams.getFitThresholdPerc())
                    .setThresholdPval(signatureParams.getFitThresholdPval())
                    .setMaxRareSigs(signatureParams.getFitMaxRareSigs())
                    .setSignaturesFile(signaturesFile)
                    .setRareSignaturesFile(rareSignaturesFile)
                    .execute();

            // Update quality control for the catalog sample
//            if (signatureParams.getQuery() != null && query.containsKey(QC_UPDATE_KEYNAME)) {
//                // Remove quality control update key
//                query.remove(QC_UPDATE_KEYNAME);
//
//                OpenCGAResult<Sample> sampleResult = getCatalogManager().getSampleManager().get(getStudy(), sample, QueryOptions.empty(),
//                        getToken());
//                Sample sample = sampleResult.first();
//                if (sample != null) {
//
//                    Signature signature = parse(getOutDir());
//                    SampleQualityControl qc = sampleResult.first().getQualityControl();
//                    if (qc == null) {
//                        qc = new SampleQualityControl();
//                    }
//                    qc.getVariant().getSignatures().add(signature);
//
//                    catalogManager.getSampleManager().update(getStudy(), sample.getId(), new SampleUpdateParams().setQualityControl(qc),
//                            QueryOptions.empty(), getToken());
//                }
//            }
        });
    }

    public static List<Signature.GenomeContextCount> parseCatalogueResults(Path dir) throws IOException {
        List<Signature.GenomeContextCount> sigCounts = null;

        // Context counts
        File contextFile = dir.resolve(CATALOGUES_FILENAME_DEFAULT).toFile();
        if (contextFile.exists()) {
            List<String> lines = FileUtils.readLines(contextFile, Charset.defaultCharset());
            sigCounts = new ArrayList<>(lines.size() - 1);
            for (int i = 1; i < lines.size(); i++) {
                String[] fields = lines.get(i).split("\t");
                sigCounts.add(new Signature.GenomeContextCount(fields[0], Math.round(Float.parseFloat((fields[1])))));
            }
        }

        return sigCounts;
    }

    public static SignatureFitting parseFittingResults(Path outDir, String fitId, String fitMethod, String fitSigVersion, Integer fitNBoot,
                                                       String fitOrgan, Float fitThresholdPerc, Float fitThresholdPval,
                                                       Integer fitMaxRareSigs) throws IOException {
        // Check for fitting coeffs. file
        File coeffsFile = outDir.resolve(SIGNATURE_COEFFS_FILENAME).toFile();
        if (!coeffsFile.exists()) {
            return null;
        }

        // Signature fitting
        SignatureFitting fitting = new SignatureFitting();
        if (StringUtils.isNotEmpty(fitId)) {
            fitting.setId(fitId);
        }
        if (StringUtils.isNotEmpty(fitMethod)) {
            fitting.setMethod(fitMethod);
        }
        if (StringUtils.isNotEmpty(fitSigVersion)) {
            fitting.setSignatureVersion(fitSigVersion);
            if (fitSigVersion.startsWith("COSMIC")) {
                fitting.setSignatureSource("COSMIC");
            } else if (fitSigVersion.startsWith("RefSig")) {
                fitting.setSignatureSource("RefSig");
            }
        }

        // Set fitting scores
        List<String> lines = FileUtils.readLines(coeffsFile, Charset.defaultCharset());
        String[] labels = lines.get(0).split("\t");
        String[] values = lines.get(1).split("\t");
        List<SignatureFittingScore> scores = new ArrayList<>(labels.length);
        for (int i = 0; i < labels.length; i++) {
            String label = labels[i];
            if (label.contains("_")) {
                String[] splits = label.split("_");
                label = splits[splits.length - 1];
            }
            scores.add(new SignatureFittingScore(label, Double.parseDouble(values[i + 1])));
        }
        fitting.setScores(scores);

        // Set files
        List<String> files = new ArrayList<>();
        for (File file : outDir.toFile().listFiles()) {
            if (file.getName().endsWith("pdf")) {
                files.add(file.getName());
            } else if (file.isDirectory()) {
                for (File file2 : file.listFiles()) {
                    if (file2.getName().endsWith("pdf")) {
                        files.add(file.getName() + "/" + file2.getName());
                    }
                }
            }
        }
        fitting.setFiles(files);

        // Set params
        ObjectMap params = new ObjectMap();
        if (fitNBoot != null) {
            params.append("nBoot", fitNBoot);
        }
        if (StringUtils.isNotEmpty(fitOrgan)) {
            params.append("organ", fitOrgan);
        }
        if (fitThresholdPerc != null) {
            params.append("thresholdPerc", fitThresholdPerc);
        }
        if (fitThresholdPval != null) {
            params.append("thresholdPval", fitThresholdPval);
        }
        if (fitMaxRareSigs != null) {
            params.append("maxRareSigs", fitMaxRareSigs);
        }
        if (params.size() > 0) {
            fitting.setParams(params);
        }
        fitting.setParams(params);

        return fitting;
    }

    public MutationalSignatureAnalysisParams getSignatureParams() {
        return signatureParams;
    }

    public MutationalSignatureAnalysis setSignatureParams(MutationalSignatureAnalysisParams signatureParams) {
        this.signatureParams = signatureParams;
        return this;
    }
}

