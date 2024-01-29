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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.Signature;
import org.opencb.biodata.models.clinical.qc.SignatureFitting;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.sample.SampleVariantQualityControlMetrics;
import org.opencb.opencga.core.models.variant.MutationalSignatureAnalysisParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.MutationalSignatureAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

@Tool(id = MutationalSignatureAnalysis.ID, resource = Enums.Resource.VARIANT)
public class MutationalSignatureAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "mutational-signature";
    public static final String DESCRIPTION = "Run mutational signature analysis for a given sample.";

    public final static String SIGNATURE_COEFFS_FILENAME = "exposures.tsv";
    public final static String CATALOGUES_FILENAME_DEFAULT = "catalogues.tsv";

    public final static String MUTATIONAL_SIGNATURE_DATA_MODEL_FILENAME = "mutational_signature.json";
    public final static String MUTATIONAL_SIGNATURE_FITTING_DATA_MODEL_FILENAME = "mutational_signature_fitting.json";

    public static final String CLUSTERED = "clustered";
    public static final String NON_CLUSTERED = "non-clustered";
    public static final String LENGTH_NA = "na";
    public static final String LENGTH_1_10Kb= "1-10Kb";
    public static final String LENGTH_10Kb_100Kb = "10-100Kb";
    public static final String LENGTH_100Kb_1Mb = "100Kb-1Mb";
    public static final String LENGTH_1Mb_10Mb = "1Mb-10Mb";
    public static final String LENGTH_10Mb = ">10Mb";
    public static final String TYPE_DEL = "del";
    public static final String TYPE_TDS = "tds";
    public static final String TYPE_INV = "inv";
    public static final String TYPE_TRANS = "trans";

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

        // Get assembly
        assembly = MutationalSignatureAnalysisUtils.getAssembly(study, catalogManager, token);

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
                query.put(VariantQueryParam.SAMPLE.key(), signatureParams.getSample());
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
        logger.info("Skip: {}", signatureParams.getSkip());
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

        // Get sample quality control again in case it was updated during the mutational signature analysis
        OpenCGAResult<Sample> sampleResult;
        try {
            sampleResult = catalogManager.getSampleManager().get(study, signatureParams.getSample(),
                    QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("After mutational signature analysis, it could not get sample from OpenCGA catalog", e);
        }
        if (sampleResult.getNumResults() != 1) {
            throw new ToolException("After mutational signature analysis, it could not get sample '" + signatureParams.getSample() + "'"
                    + " from OpenCGA catalog: number of occurrences found: " + sampleResult.getNumResults());
        }

        if (StringUtils.isEmpty(signatureParams.getId())) {
            // Nothing to do
            return;
        }

        // Only save results in sample quality control, if the signature ID is not empty
        sample = sampleResult.first();
        SampleQualityControl qc = sample.getQualityControl();

        // Sanity check
        if (qc == null) {
            qc = new SampleQualityControl();
        }
        if (qc.getVariant() == null) {
            qc.setVariant(new SampleVariantQualityControlMetrics());
        }
        if (qc.getVariant().getSignatures() == null) {
            qc.getVariant().setSignatures(new ArrayList<>());
        }

        Signature signature = null;
        SignatureFitting signatureFitting = null;
        try {
            File signatureFile = getOutDir().resolve(MUTATIONAL_SIGNATURE_DATA_MODEL_FILENAME).toFile();
            if (signatureFile.exists()) {
                signature = JacksonUtils.getDefaultObjectMapper().readerFor(Signature.class).readValue(signatureFile);
            }
            File signatureFittingFile = getOutDir().resolve(MUTATIONAL_SIGNATURE_FITTING_DATA_MODEL_FILENAME).toFile();
            if (signatureFittingFile.exists()) {
                signatureFitting = JacksonUtils.getDefaultObjectMapper().readerFor(SignatureFitting.class).readValue(signatureFittingFile);
            }
        } catch (IOException e) {
            throw new ToolException("Something happened when parsing result files from mutational signature (or fitting)", e);
        }
        if (signature != null) {
            logger.info("Adding new mutational signature to the signature data model before saving quality control");
            qc.getVariant().getSignatures().add(signature);
        }
        if (signatureFitting != null) {
            for (Signature sig : qc.getVariant().getSignatures()) {
                if (StringUtils.isNotEmpty(sig.getId())) {
                    if (sig.getId().equals(signatureParams.getId())) {
                        if (CollectionUtils.isEmpty(sig.getFittings())) {
                            sig.setFittings(new ArrayList<>());
                        }
                        logger.info("Fitting {} was added to the mutational siganture {} before saving quality control",
                                signatureParams.getFitId(), signatureParams.getId());
                        sig.getFittings().add(signatureFitting);
                        break;
                    }
                }
            }
        }
        // Update sample quality control
        try {
            catalogManager.getSampleManager().update(getStudy(), sample.getId(), new SampleUpdateParams().setQualityControl(qc),
                    QueryOptions.empty(), getToken());
            logger.info("Quality control saved for sample {}", sample.getId());
        } catch (CatalogException e) {
            throw new ToolException("Something happened when saving sample quality control", e);
        }
    }


    public MutationalSignatureAnalysisParams getSignatureParams() {
        return signatureParams;
    }

    public MutationalSignatureAnalysis setSignatureParams(MutationalSignatureAnalysisParams signatureParams) {
        this.signatureParams = signatureParams;
        return this;
    }
}

