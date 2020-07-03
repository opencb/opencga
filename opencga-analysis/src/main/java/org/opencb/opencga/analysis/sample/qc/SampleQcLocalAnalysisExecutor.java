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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.sequence.fastqc.FastQc;
import org.opencb.biodata.models.alignment.GeneCoverageStats;
import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.biodata.models.clinical.qc.Signature;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis;
import org.opencb.opencga.analysis.wrappers.executors.FastqcWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.executors.SamtoolsWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.SampleQcAnalysisExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.LOW_COVERAGE_REGION_THRESHOLD_DEFAULT;

@ToolExecutor(id="opencga-local", tool = SampleQcAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class SampleQcLocalAnalysisExecutor extends SampleQcAnalysisExecutor implements StorageToolExecutor {

    private CatalogManager catalogManager;

    @Override
    public void run() throws ToolException {
        // Sanity check: metrics to update can not be null
        if (metrics == null) {
            throw new ToolException("Sample quality control metrics is null");
        }

        catalogManager = getVariantStorageManager().getCatalogManager();

        switch (qcType) {
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
                throw new ToolException("Unknown quality control type: " + qcType);
            }
        }
    }

    private void runVariantStats() throws ToolException {
//        if (StringUtils.isEmpty(variantStatsJobId)) {
//            addWarning("Skipping sample variant stats");
//        } else {
//            OpenCGAResult<Job> jobResult;
//            try {
//                jobResult = catalogManager.getJobManager().get(studyId, Collections.singletonList(variantStatsJobId),
//                        QueryOptions.empty(), false, getToken());
//            } catch (CatalogException e) {
//                throw new ToolException(e);
//            }
//
//            Job job = jobResult.first();
//
//            Path path = Paths.get(job.getOutDir().getUri().getPath()).resolve("sample-variant-stats.json");
//            if (path.toFile().exists()) {
//                if (metrics.getVariantStats() == null) {
//                    metrics.setVariantStats(new ArrayList<>());
//                }
//
//                List<SampleVariantStats> stats = new ArrayList<>();
//                try {
//                    JacksonUtils.getDefaultObjectMapper()
//                            .readerFor(SampleVariantStats.class)
//                            .<SampleVariantStats>readValues(path.toFile())
//                            .forEachRemaining(stats::add);
//                } catch (IOException e) {
//                    throw new ToolException(e);
//                }
//
//                // Add to metrics
//                metrics.getVariantStats().add(new SampleQcVariantStats(variantStatsId, variantStatsDecription, variantStatsQuery,
//                        stats.get(0)));
//            }
//        }
    }

    private void runFastqc() throws ToolException {
        if (metrics.getFastQc() != null) {
            // FastQC already exists!
            addWarning("Skipping FastQC analysis: it was already computed");
            return;
        }

        // Check BAM file
        if (catalogBamFile == null) {
            addWarning("Skipping FastQC analysis: no BAM file was provided");
            return;
        }

        ObjectMap params = new ObjectMap();
        params.put("extract", "");

        Path outDir = getOutDir().resolve("fastqc");
        Path scratchDir = outDir.resolve("scratch");
        scratchDir.toFile().mkdirs();

        FastqcWrapperAnalysisExecutor executor = new FastqcWrapperAnalysisExecutor(getStudyId(), params, outDir, scratchDir, catalogManager,
                getToken());

        executor.setFile(catalogBamFile.getId());
        executor.run();

        // Check for result
        FastQc fastQc = executor.getResult();
        if (fastQc != null) {
            metrics.setFastQc(fastQc);
        }
    }

    private void runFlagStats() throws ToolException {
        if (metrics.getSamtoolsFlagstats() != null) {
            // Samtools flag stats already exists!
            addWarning("Skipping samtools/flagstat analysis: it was already computed");
            return;
        }

        // Check BAM file
        if (catalogBamFile == null) {
            addWarning("Skipping samtools/flagstat analysis: no BAM file was provided");
            return;
        }

        ObjectMap params = new ObjectMap();

        Path outDir = getOutDir().resolve("flagstat");
        Path scratchDir = outDir.resolve("scratch");
        scratchDir.toFile().mkdirs();

        SamtoolsWrapperAnalysisExecutor executor = new SamtoolsWrapperAnalysisExecutor(getStudyId(), params, outDir, scratchDir,
                catalogManager, getToken());

        executor.setCommand("flagstat");
        executor.setBamFile(catalogBamFile.getId());
        executor.run();

        // Check for result
        SamtoolsFlagstats flagtats = executor.getFlagstatsResult();
        if (flagtats != null) {
            metrics.setSamtoolsFlagstats(flagtats);
            System.out.println(flagtats);
        }
    }

    private void runHsMetrics() throws ToolException {
        addWarning("Skipping picard/CollectHsMetrics analysis: not yet implemented");

        //        if (metrics.getHsMetricsReport() != null) {
//            // Hs metrics already exists!
//            addWarning("Skipping picard/CollectHsMetrics analysis: it was already computed");
//            return;
//        }
//
//        // Check BAM file
//        File bamFile = getBamFileFromCatalog();
//        if (bamFile == null) {
//            addWarning("Skipping picard/CollectHsMetrics analysis: no BAM file was provided and no BAM file found for sample " + sample.getId());
//            return;
//        }
//
//        // Check bait file
//        if (StringUtils.isEmpty(getBaitFile())) {
//            addWarning("Skipping picard/CollectHsMetrics analysis: no bait file was provided");
//        }
//        File baitFile = AnalysisUtils.getCatalogFile(getBaitFile(), getStudyId(), catalogManager.getFileManager(), getToken());
//        if (baitFile == null) {
//            addWarning("Skipping picard/CollectHsMetrics analysis: no bait file '" + getBaitFile() + "' found in catalog database");
//            return;
//        }
//
//        // Check target file
//        if (StringUtils.isEmpty(getTargetFile())) {
//            addWarning("Skipping picard/CollectHsMetrics analysis: no target file was provided");
//        }
//        File targetFile = AnalysisUtils.getCatalogFile(getTargetFile(), getStudyId(), catalogManager.getFileManager(), getToken());
//        if (targetFile == null) {
//            addWarning("Skipping picard/CollectHsMetrics analysis: no target file '" + getBaitFile() + "' found in catalog database");
//            return;
//        }
//
//        // Check fasta file
//        if (StringUtils.isEmpty(getFastaFile())) {
//            addWarning("Skipping picard/CollectHsMetrics analysis: no fasta file was provided");
//        }
//        File fastaFile = AnalysisUtils.getCatalogFile(getFastaFile(), getStudyId(), catalogManager.getFileManager(), getToken());
//        if (fastaFile == null) {
//            addWarning("Skipping picard/CollectHsMetrics analysis: no fasta file '" + getFastaFile() + "' found in catalog database");
//            return;
//        }


//        ObjectMap params = new ObjectMap();
//        params.put("INPUT", getBamFile());
//
//        Path outDir = getOutDir().resolve("/fastqc");
//        Path scratchDir = outDir.resolve("/scratch");
//
//        FastqcWrapperAnalysisExecutor executor = new FastqcWrapperAnalysisExecutor(getStudyId(), params, outDir, scratchDir, catalogManager,
//                getToken());
//
//        executor.setFile(getBamFile());
//        executor.run();
    }

    private void runGeneCoverageStats() throws ToolException {
        // Check BAM file
        if (catalogBamFile == null) {
            addWarning("Skipping gene coverage stats analysis: no BAM file was provided");
            return;
        }

        // Check genes
        if (CollectionUtils.isEmpty(getGenesForCoverageStats())) {
            addWarning("Skipping gene coverage stats analysis: missing genes");
            return;
        }

        // Sanity check
        List<GeneCoverageStats> geneCoverageStats = metrics.getGeneCoverageStats();
        if (geneCoverageStats == null) {
            geneCoverageStats = new ArrayList<>();
        }

        List<String> targetGenes = new ArrayList<>();
        if (CollectionUtils.isEmpty(geneCoverageStats)) {
            targetGenes = getGenesForCoverageStats();
        } else {
            for (String gene : getGenesForCoverageStats()) {
                boolean found = false;
                for (GeneCoverageStats stats : geneCoverageStats) {
                    if (gene.equals(stats.getGeneName())) {
                        found = true;
                        addWarning("Skipping gene coverage stats for gene " + gene + ": it was already computed");
                        break;
                    }
                }
                if (!found) {
                    targetGenes.add(gene);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(targetGenes)) {
            try {
                OpenCGAResult<GeneCoverageStats> geneCoverageStatsResult = getAlignmentStorageManager().coverageStats(getStudyId(),
                        catalogBamFile.getId(), targetGenes, Integer.parseInt(LOW_COVERAGE_REGION_THRESHOLD_DEFAULT), getToken());

                if (geneCoverageStatsResult.getNumResults() != 1) {
                    throw new ToolException("Something wrong happened when computing gene coverage stats: no results returned");
                }
                geneCoverageStats.add(geneCoverageStatsResult.first());

                // Add result to the list
                metrics.setGeneCoverageStats(geneCoverageStats);
            } catch (Exception e) {
                throw new ToolException(e);
            }
        }
    }

    private void runMutationalSignature() throws ToolException {
//        if (!sample.isSomatic()) {
//            addWarning("Skipping mutational signature analysis: sample " + sample.getId() + " is not somatic");
//        } else {
//            if (metrics.getSignatures() == null) {
//                metrics.setSignatures(new ArrayList<>());
//            }
//
//            if (StringUtils.isEmpty(variantStatsJobId)) {
//                // mutationalSignature/query
//                addWarning("Skipping mutational signature analysis: not yet implemented mutationalSignature/query");
//            } else {
//                // mutationalSignature/run
//                OpenCGAResult<Job> jobResult;
//                try {
//                    jobResult = catalogManager.getJobManager().get(studyId, Collections.singletonList(variantStatsJobId),
//                            QueryOptions.empty(), false, getToken());
//                } catch (CatalogException e) {
//                    throw new ToolException(e);
//                }
//
//                Job job = jobResult.first();
//
//                Path path = Paths.get(job.getOutDir().getUri().getPath()).resolve("context.txt");
//                if (path.toFile().exists()) {
//                    Signature.SignatureCount[] signatureCounts = MutationalSignatureAnalysis.parseSignatureCounts(path.toFile());
//
//                    // Add to metrics
//                    metrics.getSignatures().add(new Signature(signatureId, signatureQuery, "SNV", signatureCounts, new ArrayList()));
//                }
//            }
//        }
    }
}
