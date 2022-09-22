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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
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

    public final static String GENOME_CONTEXT_FILENAME = "genome_context.txt";
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

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        // Two behaviours: using catalogues or using sample/query
        if (StringUtils.isNotEmpty(signatureParams.getCatalogues())) {
            // Fitting from file containing the counts
            // Check if that file exists
            OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult = getCatalogManager().getFileManager().get(study,
                    signatureParams.getCatalogues(), QueryOptions.empty(), getToken());
            if (fileResult.getNumResults() == 0) {
                throw new ToolException("Catalogues file '" + signatureParams.getCatalogues() + "' does not exist in study '"
                        + study + "'");
            }
            if (fileResult.getNumResults() > 1) {
                throw new ToolException("Multiple files '" + signatureParams.getCatalogues() + "' found in study '" + study + "'");
            }
            catalogues = fileResult.first().getUri().toURL().getPath();
            logger.info("Signagture catalogues file: {}", catalogues);
        } else if (StringUtils.isNotEmpty(signatureParams.getCataloguesContent())) {
            // Fitting from counts
            FileUtils.write(getOutDir().resolve(CATALOGUES_FILENAME_DEFAULT).toFile(), signatureParams.getCataloguesContent(),
                    Charset.defaultCharset(), false);
            catalogues = getOutDir().resolve(CATALOGUES_FILENAME_DEFAULT).toString();
            logger.info("Signagture catalogues file: {}", catalogues);
        } else {
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
                study = catalogManager.getStudyManager().get(study, QueryOptions.empty(), token).first().getFqn();

                OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(study, sample, QueryOptions.empty(), token);
                if (sampleResult.getNumResults() != 1) {
                    throw new ToolException("Unable to compute mutational signature analysis. Sample '" + sample + "' not found");
                }
            } catch (CatalogException e) {
                throw new ToolException(e);
            }
        }

        // Log messages
        logger.info("Signagture fitting method: {}", signatureParams.getFitMethod());
        logger.info("Signagture sig. version: {}", signatureParams.getSigVersion());
        logger.info("Signagture organ: {}", signatureParams.getOrgan());
        logger.info("Signagture n boot: {}", signatureParams.getnBoot());
        logger.info("Signagture threshold percentage: {}", signatureParams.getThresholdPerc());
        logger.info("Signagture threshold p-value: {}", signatureParams.getThresholdPval());
        logger.info("Signagture max. rare sigs.: {}", signatureParams.getMaxRareSigs());
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
                    .setSigVersion(signatureParams.getSigVersion())
                    .setOrgan(signatureParams.getOrgan())
                    .setnBoot(signatureParams.getnBoot())
                    .setThresholdPerc(signatureParams.getThresholdPerc())
                    .setThresholdPval(signatureParams.getThresholdPval())
                    .setMaxRareSigs(signatureParams.getMaxRareSigs())
                    .execute();

            // Update quality control for the catalog sample
            if (signatureParams.getQuery() != null && query.containsKey(QC_UPDATE_KEYNAME)) {
                // Remove quality control update key
                query.remove(QC_UPDATE_KEYNAME);

                OpenCGAResult<Sample> sampleResult = getCatalogManager().getSampleManager().get(getStudy(), sample, QueryOptions.empty(),
                        getToken());
                Sample sample = sampleResult.first();
                if (sample != null) {

                    Signature signature = parse(getOutDir());
                    SampleQualityControl qc = sampleResult.first().getQualityControl();
                    if (qc == null) {
                        qc = new SampleQualityControl();
                    }
                    qc.getVariant().getSignatures().add(signature);

                    catalogManager.getSampleManager().update(getStudy(), sample.getId(), new SampleUpdateParams().setQualityControl(qc),
                            QueryOptions.empty(), getToken());
                }
            }
        });
    }

    public Signature parse(Path dir) throws IOException {
        Signature result = new Signature(signatureParams.getId(), signatureParams.getDescription(), query, "SNV", null, null, null);

        // Context counts
        File contextFile = dir.resolve(GENOME_CONTEXT_FILENAME).toFile();
        if (contextFile.exists()) {
            List<String> lines = FileUtils.readLines(contextFile, Charset.defaultCharset());
            List<Signature.GenomeContextCount> sigCounts = new ArrayList<>(lines.size() - 1);
            for (int i = 1; i < lines.size(); i++) {
                String[] fields = lines.get(i).split("\t");
                sigCounts.add(new Signature.GenomeContextCount(fields[2], Math.round(Float.parseFloat((fields[3])))));
            }
            result.setCounts(sigCounts);
        }

        // Signature fitting
        File coeffsFile = dir.resolve(SIGNATURE_COEFFS_FILENAME).toFile();
        if (coeffsFile.exists()) {
            SignatureFitting fitting = new SignatureFitting()
                    .setMethod(signatureParams.getFitMethod())
                    .setSignatureVersion(signatureParams.getSigVersion());

            // Set source from fit method
            if (StringUtils.isNotEmpty(getSignatureParams().getSigVersion())) {
                if (getSignatureParams().getSigVersion().startsWith("COSMIC")) {
                    fitting.setSignatureSource("COSMIC");
                } else if (getSignatureParams().getSigVersion().startsWith("RefSig")) {
                    fitting.setSignatureSource("RefSig");
                }
            }

            // Set fitting scores
            List<String> lines = FileUtils.readLines(coeffsFile, Charset.defaultCharset());
            String[] labels = lines.get(0).split("\t");
            String[] values = lines.get(1).split("\t");
            List<SignatureFitting.Score> scores = new ArrayList<>(labels.length);
            for (int i = 0; i < labels.length; i++) {
                String label = labels[i];
                if (label.contains("_")) {
                    String[] splits = label.split("_");
                    label = splits[splits.length - 1];
                }
                scores.add(new SignatureFitting.Score(label, Double.parseDouble(values[i + 1])));
            }
            fitting.setScores(scores);

            // Set files
            List<String> files = new ArrayList<>();
            for (File file : getOutDir().toFile().listFiles()) {
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
            if (signatureParams.getnBoot() != null) {
                params.append("nBoot", signatureParams.getnBoot());
            }
            if (StringUtils.isNotEmpty(signatureParams.getOrgan())) {
                params.append("organ", signatureParams.getOrgan());
            }
            if (signatureParams.getThresholdPerc() != null) {
                params.append("thresholdPerc", signatureParams.getThresholdPerc());
            }
            if (signatureParams.getThresholdPval() != null) {
                params.append("thresholdPval", signatureParams.getThresholdPval());
            }
            if (signatureParams.getMaxRareSigs() != null) {
                params.append("maxRareSigs", signatureParams.getMaxRareSigs());
            }
            if (params.size() > 0) {
                fitting.setParams(params);
            }
            fitting.setParams(params);

            // Set fitting signature
            result.setFitting(fitting);
        }

        return result;
    }

    public MutationalSignatureAnalysisParams getSignatureParams() {
        return signatureParams;
    }

    public MutationalSignatureAnalysis setSignatureParams(MutationalSignatureAnalysisParams signatureParams) {
        this.signatureParams = signatureParams;
        return this;
    }
}

