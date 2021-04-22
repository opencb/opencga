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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.alignment.picard.HsMetrics;
import org.opencb.biodata.formats.alignment.picard.io.HsMetricsParser;
import org.opencb.biodata.models.alignment.GeneCoverageStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.wrappers.picard.PicardWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.SampleQcAnalysisExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.LOW_COVERAGE_REGION_THRESHOLD_DEFAULT;

@ToolExecutor(id="opencga-local", tool = SampleQcAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class SampleQcLocalAnalysisExecutor extends SampleQcAnalysisExecutor implements StorageToolExecutor {

    private CatalogManager catalogManager;

    @Override
    public void run() throws ToolException {
        // Sanity check: metrics to update can not be null
        if (alignmentQcMetrics == null) {
            throw new ToolException("Sample alignment quality control metrics is null");
        }

        catalogManager = getVariantStorageManager().getCatalogManager();

        switch (qcType) {

            case GENE_COVERAGE_STATS: {
                runGeneCoverageStats();
                break;
            }

            default: {
                throw new ToolException("Unknown quality control type: " + qcType);
            }
        }
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
        List<GeneCoverageStats> geneCoverageStats = alignmentQcMetrics.getGeneCoverageStats();
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
                alignmentQcMetrics.setGeneCoverageStats(geneCoverageStats);
            } catch (Exception e) {
                throw new ToolException(e);
            }
        }
    }
}
