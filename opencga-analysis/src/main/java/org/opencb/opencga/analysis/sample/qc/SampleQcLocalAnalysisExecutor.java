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

import org.apache.commons.io.FileUtils;
import org.opencb.biodata.formats.sequence.fastqc.FastQc;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.wrappers.executors.FastqcWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.SampleQcAnalysisExecutor;
import org.parboiled.common.StringUtils;

import java.io.IOException;
import java.nio.file.Path;

@ToolExecutor(id="opencga-local", tool = SampleQcAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class SampleQcLocalAnalysisExecutor extends SampleQcAnalysisExecutor implements StorageToolExecutor {

    private Sample sample;

    private SampleQualityControl qc;
    private CatalogManager catalogManager;

    @Override
    public void run() throws ToolException {

        sample = getSample();
        qc = sample.getQualityControl();
        if (qc == null) {
            qc = new SampleQualityControl();
        }

        catalogManager = getVariantStorageManager().getCatalogManager();

        switch (getQc()) {
            case VARIAN_STATS: {
                runVariantStats();
                break;
            }

            case FASTQC: {
                runFastqc();
                break;
            }

            case FLAG_STATS: {
                runFlagStats();
                break;
            }

            case HS_METRICS: {
                runHsMetrics();
                break;
            }

            case GENE_COVERAGE_STATS: {
                runGeneCoverageStats();
                break;
            }

            case MUTATIONAL_SIGNATURE: {
                runMutationalSignature();
                break;
            }
            default: {
                throw new ToolException("Unknown quality control: " + getQc());
            }
        }
    }

    private void runVariantStats() throws ToolException {
    }

    private void runFastqc() throws ToolException {
        if (qc.getFastQc() != null) {
            // FastQC already exists!
            addWarning("Skipping FastQC analysis: it was already computed");
            return;
        }

        // Check BAM file
        if (StringUtils.isEmpty(getBamFile())) {
            addWarning("Skipping picard/CollectHsMetrics analysis: no BAM file was provided");
        }
        File bamFile = AnalysisUtils.getCatalogFile(getBamFile(), getStudyId(), catalogManager.getFileManager(), getToken());
        if (bamFile == null) {
            addWarning("Skipping FastQC analysis: missing BAM file '" + getBamFile() + "' in catalog database");
            return;
        }

        ObjectMap params = new ObjectMap();
        params.put("extract", "");

        Path outDir = getOutDir().resolve("fastqc");
        Path scratchDir = outDir.resolve("scratch");
        scratchDir.toFile().mkdirs();

        FastqcWrapperAnalysisExecutor executor = new FastqcWrapperAnalysisExecutor(getStudyId(), params, outDir, scratchDir, catalogManager,
                getToken());

        executor.setFile(getBamFile());
        executor.run();

        // Check for result
        FastQc fastQc = executor.getResult();
        if (fastQc != null) {
            qc.setFastQc(fastQc);
            System.out.println(fastQc);
        }
    }

    private void runFlagStats() throws ToolException {
        if (qc.getSamtoolsFlagStatsReport() != null) {
            // Samtools flag stats already exists!
            addWarning("Skipping samtools/flagstat analysis: it was already computed");
            return;
        }

        CatalogManager catalogManager = getVariantStorageManager().getCatalogManager();

        // Check BAM file
        if (StringUtils.isEmpty(getBaitFile())) {
            addWarning("Skipping picard/CollectHsMetrics analysis: no bait file was provided");
        }
        File baitFile = AnalysisUtils.getCatalogFile(getBaitFile(), getStudyId(), catalogManager.getFileManager(), getToken());

        ObjectMap params = new ObjectMap();

        Path outDir = getOutDir().resolve("flagstat");
        Path scratchDir = outDir.resolve("scratch");

//        SamtoolsWrapperAnalysisExecutor executor = new SamtoolsWrapperAnalysisExecutor(getStudyId(), params, outDir, scratchDir,
//                catalogManager, getToken());
//
//        executor.setFile(getBamFile());
//        executor.run();
    }

    private void runHsMetrics() throws ToolException {
        if (qc.getHsMetricsReport() != null) {
            // Hs metrics already exists!
            addWarning("Skipping picard/CollectHsMetrics analysis: it was already computed");
            return;
        }

        // Check BAM file
        File bamFile = getBamFileFromCatalog();
        if (bamFile == null) {
            addWarning("Skipping picard/CollectHsMetrics analysis: no BAM file was provided and no BAM file found for sample " + sample.getId());
            return;
        }

        // Check bait file
        if (StringUtils.isEmpty(getBaitFile())) {
            addWarning("Skipping picard/CollectHsMetrics analysis: no bait file was provided");
        }
        File baitFile = AnalysisUtils.getCatalogFile(getBaitFile(), getStudyId(), catalogManager.getFileManager(), getToken());
        if (baitFile == null) {
            addWarning("Skipping picard/CollectHsMetrics analysis: no bait file '" + getBaitFile() + "' found in catalog database");
            return;
        }

        // Check target file
        if (StringUtils.isEmpty(getTargetFile())) {
            addWarning("Skipping picard/CollectHsMetrics analysis: no target file was provided");
        }
        File targetFile = AnalysisUtils.getCatalogFile(getTargetFile(), getStudyId(), catalogManager.getFileManager(), getToken());
        if (targetFile == null) {
            addWarning("Skipping picard/CollectHsMetrics analysis: no target file '" + getBaitFile() + "' found in catalog database");
            return;
        }

        // Check fasta file
        if (StringUtils.isEmpty(getFastaFile())) {
            addWarning("Skipping picard/CollectHsMetrics analysis: no fasta file was provided");
        }
        File fastaFile = AnalysisUtils.getCatalogFile(getFastaFile(), getStudyId(), catalogManager.getFileManager(), getToken());
        if (fastaFile == null) {
            addWarning("Skipping picard/CollectHsMetrics analysis: no fasta file '" + getFastaFile() + "' found in catalog database");
            return;
        }

        ObjectMap params = new ObjectMap();
        params.put("INPUT", getBamFile());

        Path outDir = getOutDir().resolve("/fastqc");
        Path scratchDir = outDir.resolve("/scratch");

        FastqcWrapperAnalysisExecutor executor = new FastqcWrapperAnalysisExecutor(getStudyId(), params, outDir, scratchDir, catalogManager,
                getToken());

        executor.setFile(getBamFile());
        executor.run();
    }

    private void runGeneCoverageStats() throws ToolException {
    }

    private void runMutationalSignature() throws ToolException {
    }

    private File getBamFileFromCatalog() {
        File file;
        if (StringUtils.isEmpty(getBamFile())) {
            try {
                file = AnalysisUtils.getBamFileBySampleId(sample.getId(), getStudyId(), catalogManager.getFileManager(), getToken());
            } catch (ToolException e) {
                // FastQC already exists!
                return null;
            }
        } else {
            try {
                file = AnalysisUtils.getBamFile(getBamFile(), sample.getId(), getStudyId(), catalogManager.getFileManager(), getToken());
            } catch (ToolException e) {
                return null;
            }
        }

        return file;
    }

}
