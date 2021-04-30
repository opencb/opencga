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

package org.opencb.opencga.analysis.alignment.qc;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.alignment.GeneCoverageStats;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.AlignmentGeneCoverageStatsParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileQualityControl;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.tools.alignment.AlignmentGeneCoverageStatsAnalysisExecutor;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.ALIGNMENT_GENE_COVERAGE_STATS_DESCRIPTION;

@Tool(id = AlignmentGeneCoverageStatsAnalysis.ID, resource = Enums.Resource.ALIGNMENT)
public class AlignmentGeneCoverageStatsAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "gene-coverage-stats";
    public static final String DESCRIPTION = ALIGNMENT_GENE_COVERAGE_STATS_DESCRIPTION;
    public static final int GENE_LIST_MAXIMUM_SIZE = 10;

    private static final String COMPUTE_GENE_COVERAGE_STEP = "compute-gene-coverage-stats";
    private static final String UPDATE_QUAlITY_CONTROL_STEP = "update-quality-control";

    private File catalogBamFile;
    private List<String> targetGenes;

    @ToolParams
    protected final AlignmentGeneCoverageStatsParams analysisParams = new AlignmentGeneCoverageStatsParams();

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        try {
            catalogBamFile = AnalysisUtils.getCatalogFile(analysisParams.getBamFile(), study, catalogManager.getFileManager(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error accessing to the BAM file '" + analysisParams.getBamFile() + "'", e);
        }

        if (CollectionUtils.isEmpty(analysisParams.getGenes())) {
            throw new ToolException("Gene list is empty");
        }

        if (analysisParams.getGenes().size() > GENE_LIST_MAXIMUM_SIZE) {
            throw new ToolException("The gene list exceeds the maximum size of " + GENE_LIST_MAXIMUM_SIZE + " genes. The input list"
                    + " contains " + analysisParams.getGenes().size() + " genes");
        }

        // Remove duplicated genes
        targetGenes = new ArrayList<>(new HashSet<>(analysisParams.getGenes()));
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        steps.add(COMPUTE_GENE_COVERAGE_STEP);
        steps.add(UPDATE_QUAlITY_CONTROL_STEP);
        return steps;
    }

    @Override
    protected void run() throws ToolException {

        setUpStorageEngineExecutor(study);

        AlignmentGeneCoverageStatsAnalysisExecutor executor = getToolExecutor(AlignmentGeneCoverageStatsAnalysisExecutor.class)
                .setStudyId(getStudy())
                .setBamFileId(catalogBamFile.getId())
                .setGenes(targetGenes);

        step(COMPUTE_GENE_COVERAGE_STEP, () -> {
            executor.execute();
        });

        step(UPDATE_QUAlITY_CONTROL_STEP, () -> {
            List<GeneCoverageStats> geneCoverageStats = executor.getGeneCoverageStats();

            if (CollectionUtils.isNotEmpty(geneCoverageStats)) {
                // Update quality control for the catalog file
                FileQualityControl qc = catalogBamFile.getQualityControl();

                // Sanity check
                if (qc == null) {
                    qc = new FileQualityControl();
                }
                qc.getCoverageQualityControl().setGeneCoverageStats(geneCoverageStats);

                catalogManager.getFileManager().update(getStudy(), catalogBamFile.getId(), new FileUpdateParams().setQualityControl(qc),
                        QueryOptions.empty(), getToken());
            }
        });
    }
}
