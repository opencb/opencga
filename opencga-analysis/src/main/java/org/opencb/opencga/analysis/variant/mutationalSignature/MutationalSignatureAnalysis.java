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

import org.apache.commons.collections4.CollectionUtils;
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
import java.util.Locale;

@Tool(id = MutationalSignatureAnalysis.ID, resource = Enums.Resource.VARIANT)
public class MutationalSignatureAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "mutational-signature";
    public static final String DESCRIPTION = "Run mutational signature analysis for a given sample.";

    public final static String SIGNATURE_COEFFS_FILENAME = "exposures.tsv";
    public final static String CATALOGUES_FILENAME_DEFAULT = "catalogues.tsv";

    @ToolParams
    private MutationalSignatureAnalysisParams signatureParams = new MutationalSignatureAnalysisParams();

    private Sample sample;
    private String assembly;
    private ObjectMap query;
    private String signaturesFile;
    private String rareSignaturesFile;

    private boolean runCatalogue = true;
    private boolean runFitting = true;

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        if (StringUtils.isEmpty(signatureParams.getSample())) {
            throw new ToolException("Missing sample. It is mandatory to run mutational signature analysis");
        }

        // Check sample
        study = catalogManager.getStudyManager().get(study, QueryOptions.empty(), token).first().getFqn();
        OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(study, signatureParams.getSample(),
                QueryOptions.empty(), token);
        if (sampleResult.getNumResults() != 1) {
            throw new ToolException("Unable to compute mutational signature analysis. Sample '" + signatureParams.getSample()
                    + "' not found");
        }
        sample = sampleResult.first();

        if (StringUtils.isNotEmpty(signatureParams.getSkip())) {
            if (signatureParams.getSkip().contains(MutationalSignatureAnalysisParams.SIGNATURE_CATALOGUE_SKIP_VALUE)) {
                runCatalogue = false;
            }
            if (signatureParams.getSkip().contains(MutationalSignatureAnalysisParams.SIGNATURE_FITTING_SKIP_VALUE)) {
                runFitting = false;
            }
        }

        // Check 'catalogue' processing
        if (runCatalogue) {
            if (signatureParams.getQuery() == null) {
                throw new ToolException("Missing signature query. It is mandatory to compute mutational signature catalogue");
            }

            query = JacksonUtils.getDefaultObjectMapper().readValue(signatureParams.getQuery(), ObjectMap.class);
            if (!query.containsKey(VariantQueryParam.SAMPLE.key())
                    || StringUtils.isEmpty(query.getString(VariantQueryParam.SAMPLE.key()))) {
                query.put(VariantQueryParam.SAMPLE.key(), signatureParams.getQuery());
            } else {
                // Check mismatch sample
                String tmpSample = query.getString(VariantQueryParam.SAMPLE.key());
                if (tmpSample.contains(":")) {
                    tmpSample = tmpSample.split(":")[0];
                }
                if (!tmpSample.equals(signatureParams.getSample())) {
                    throw new ToolException("Mismatch sample name, from sample parameter (" + signatureParams.getSample() + ", and from"
                            + " the query (" + query.getString(VariantQueryParam.SAMPLE.key()) + ")");
                }
            }
            logger.info("Signagture query: {}", signatureParams.getQuery());
        }

        // Check 'fitting' processing
        if (runFitting) {
            if (StringUtils.isEmpty(signatureParams.getId())) {
                throw new ToolException("Missing signature catalogue ID (counts ID). It is mandatory to compute signature fitting");
            }

            // Check that signature (catalogue) ID exists for the sample
            boolean found = false;
            if (sample.getQualityControl() != null && sample.getQualityControl().getVariant() != null
                    && CollectionUtils.isNotEmpty(sample.getQualityControl().getVariant().getSignatures())) {
                for (Signature signature : sample.getQualityControl().getVariant().getSignatures()) {
                    if (signatureParams.getId().equals(signature.getId())) {
                        found = true;
                        break;
                    }
                }
            }
            if (!found && !runCatalogue) {
                throw new ToolException("Signature catalogue ID (counts ID) '" + signatureParams.getId() + "' not found for the sample"
                        + "'" + signatureParams.getSample() + "'");
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

        // Log messages
        logger.info("Signagture id: {}", signatureParams.getId());
        logger.info("Signagture description: {}", signatureParams.getDescription());
        logger.info("Signagture sample: {}", signatureParams.getSample());
        logger.info("Signagture query: {}", signatureParams.getQuery());
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
                    .setSample(signatureParams.getSample())
                    .setAssembly(assembly)
                    .setQueryId(signatureParams.getId())
                    .setQueryDescription(signatureParams.getDescription())
                    .setSample(signatureParams.getSample())
                    .setQuery(query)
                    .setFitId(signatureParams.getFitId())
                    .setFitMethod(signatureParams.getFitMethod())
                    .setSigVersion(signatureParams.getFitSigVersion())
                    .setOrgan(signatureParams.getFitOrgan())
                    .setnBoot(signatureParams.getFitNBoot())
                    .setThresholdPerc(signatureParams.getFitThresholdPerc())
                    .setThresholdPval(signatureParams.getFitThresholdPval())
                    .setMaxRareSigs(signatureParams.getFitMaxRareSigs())
                    .setSignaturesFile(signaturesFile)
                    .setRareSignaturesFile(rareSignaturesFile)
                    .setSkip(signatureParams.getSkip())
                    .execute();
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

