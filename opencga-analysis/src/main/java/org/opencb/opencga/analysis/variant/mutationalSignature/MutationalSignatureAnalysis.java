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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.MutationalSignatureAnalysisExecutor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Tool(id = MutationalSignatureAnalysis.ID, resource = Enums.Resource.VARIANT)
public class MutationalSignatureAnalysis extends OpenCgaTool {

    public static final String ID = "mutational-signature";
    public static final String DESCRIPTION = "Run mutational signature analysis for a given sample.";

    public final static String SIGNATURES_FILENAME = "signatures_probabilities_v2.txt";

    private String study;
    private String sampleName;

    private Path refGenomePath;
    private Path mutationalSignaturePath;

    /**
     * Study of the sample.
     * @param study Study id
     * @return this
     */
    public MutationalSignatureAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    /**
     * Sample.
     * @param sampleName Sample name
     * @return this
     */
    public MutationalSignatureAnalysis setSampleName(String sampleName) {
        this.sampleName = sampleName;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        if (study == null || study.isEmpty()) {
            throw new ToolException("Missing study");
        }
        try {
            study = catalogManager.getStudyManager().get(study, null, token).first().getFqn();

            if (StringUtils.isNotEmpty(sampleName)) {
                OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(study, sampleName, new QueryOptions(), token);
                if (sampleResult.getNumResults() != 1) {
                    throw new ToolException("Unable to compute mutational signature analysis. Sample '" + sampleName + "' not found");
                }
            }
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        addAttribute("sampleName", sampleName);
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        steps.add("download-ref-genomes");
        steps.add("download-mutational-signatures");
        steps.add(getId());
        return steps;
    }

    @Override
    protected void run() throws ToolException {
        step("download-ref-genomes", () -> {
            // FIXME to make URLs dependent on assembly (and Ensembl/NCBI ?)
            ResourceUtils.DownloadedRefGenome refGenome = ResourceUtils.downloadRefGenome(ResourceUtils.Species.hsapiens,
                    ResourceUtils.Assembly.GRCh38, ResourceUtils.Authority.Ensembl, getScratchDir(), getOpencgaHome());

            if (refGenome == null) {
                throw new ToolException("Something wrong happened downloading reference genome from " + ResourceUtils.URL);
            }

            refGenomePath = refGenome.getGzFile().toPath();
        });

        step("download-mutational-signatures", () -> {
            File signatureFile = ResourceUtils.downloadAnalysis(MutationalSignatureAnalysis.ID, SIGNATURES_FILENAME, getOutDir(),
                    getOpencgaHome());
            if (signatureFile == null) {
                throw new ToolException("Error downloading mutational signatures file from " + ResourceUtils.URL);
            }

            mutationalSignaturePath = signatureFile.toPath();
        });

        step(getId(), () -> {
            getToolExecutor(MutationalSignatureAnalysisExecutor.class)
                    .setStudy(study)
                    .setSampleName(sampleName)
                    .setRefGenomePath(refGenomePath)
                    .setMutationalSignaturePath(mutationalSignaturePath)
                    .setOpenCgaHome(getOpencgaHome())
                    .execute();
        });
    }

    public static Signature.SignatureCount[] parseSignatureCounts(File contextFile) throws ToolException {
        try {
            Signature.SignatureCount[] sigCounts = null;
            if (contextFile.exists()) {
                List<String> lines = FileUtils.readLines(contextFile, Charset.defaultCharset());
                sigCounts = new Signature.SignatureCount[lines.size() - 1];
                for (int i = 1; i < lines.size(); i++) {
                    String[] fields = lines.get(i).split("\t");
                    sigCounts[i - 1] = new Signature.SignatureCount(fields[2], Math.round(Float.parseFloat((fields[3]))));
                }
            }
            return sigCounts;
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }

}

