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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Tool(id = MutationalSignatureAnalysis.ID, resource = Enums.Resource.VARIANT)
public class MutationalSignatureAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "mutational-signature";
    public static final String DESCRIPTION = "Run mutational signature analysis for a given sample.";

    public final static String GENOME_CONTEXT_FILENAME = "genome_context.txt";
    public final static String SIGNATURE_COEFFS_FILENAME = "signature_coefficients.json";
    public final static String SIGNATURE_FITTING_FILENAME = "signature_summary.png";

    public final static String QC_UPDATE_KEYNAME = "qcUpdate";

    @ToolParams
    private MutationalSignatureAnalysisParams signatureParams = new MutationalSignatureAnalysisParams();

    private String assembly;

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        if (signatureParams.getQuery() == null) {
            throw new ToolException("Missing signature query");
        }

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

            OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(study, signatureParams.getSample(),
                    QueryOptions.empty(), token);
            if (sampleResult.getNumResults() != 1) {
                throw new ToolException("Unable to compute mutational signature analysis. Sample '" + signatureParams.getSample()
                        + "' not found");
            }
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    @Override
    protected void run() throws ToolException {
        step(getId(), () -> {
            getToolExecutor(MutationalSignatureAnalysisExecutor.class)
                    .setStudy(study)
                    .setAssembly(assembly)
                    .setSample(signatureParams.getSample())
                    .setQueryId(signatureParams.getId())
                    .setQueryDescription(signatureParams.getDescription())
                    .setQuery(signatureParams.getQuery())
                    .setRelease(signatureParams.getRelease())
                    .setFitting(signatureParams.isFitting())
                    .execute();

            // Update quality control for the catalog sample
            if (signatureParams.getQuery() != null && signatureParams.getQuery().containsKey(QC_UPDATE_KEYNAME)) {
                // Remove quality control update key
                signatureParams.getQuery().remove(QC_UPDATE_KEYNAME);

                OpenCGAResult<Sample> sampleResult = getCatalogManager().getSampleManager().get(getStudy(), signatureParams.getSample(),                        QueryOptions.empty(), getToken());
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
        Signature result = new Signature(signatureParams.getId(), signatureParams.getDescription(),
                signatureParams.getQuery(), "SNV", null, Collections.emptyList(), null);

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

        // Signatures coefficients
        File coeffsFile = dir.resolve(SIGNATURE_COEFFS_FILENAME).toFile();
        if (coeffsFile.exists()) {
            SignatureFitting fitting = new SignatureFitting()
                    .setMethod("GEL")
                    .setSignatureSource("Cosmic")
                    .setSignatureVersion(signatureParams.getRelease());

            Map content = JacksonUtils.getDefaultObjectMapper().readValue(coeffsFile, Map.class);
            Map coefficients = (Map) content.get("coefficients");
            List<SignatureFitting.Score> scores = new ArrayList<>(coefficients.size());
            for (Object key : coefficients.keySet()) {
                Number coeff = (Number) coefficients.get(key);
                scores.add(new SignatureFitting.Score((String) key, coeff.doubleValue()));
            }
            fitting.setScores(scores);
            fitting.setCoeff((Double) content.get("rss"));

            // Signature summary image
            File imgFile = dir.resolve(SIGNATURE_FITTING_FILENAME).toFile();
            if (imgFile.exists()) {
                int index = imgFile.getAbsolutePath().indexOf("JOBS/");
                String relativeFilePath = (index == -1 ? imgFile.getName() : imgFile.getAbsolutePath().substring(index));
                fitting.setImage(relativeFilePath);
            }

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

