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

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.alignment.picard.HsMetrics;
import org.opencb.biodata.formats.alignment.picard.io.HsMetricsParser;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.wrappers.picard.PicardWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.AlignmentHsMetricsParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileQualityControl;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.ALIGNMENT_HS_METRICS_DESCRIPTION;
import static org.opencb.opencga.core.tools.OpenCgaToolExecutor.EXECUTOR_ID;

@Tool(id = AlignmentHsMetricsAnalysis.ID, resource = Enums.Resource.ALIGNMENT)
public class AlignmentHsMetricsAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "alignment-hsmetrics";
    public static final String DESCRIPTION = ALIGNMENT_HS_METRICS_DESCRIPTION;

    public static final String HS_METRICS_FILENAME = "hsmetrics.txt";

    private static final String PICARD_BEDTOINTERVALLIST_STEP = "picard-bed-to-interval-list";
    private static final String PICARD_COLLECTHSMETRICS_STEP = "picard-collect-hs-metrics";

    @ToolParams
    protected final AlignmentHsMetricsParams analysisParams = new AlignmentHsMetricsParams();

    private File catalogBamFile;
    private File catalogBedFile;
    private File catalogDictFile;
//    private File catalogRefSeqFile;

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        try {
            catalogBamFile = AnalysisUtils.getCatalogFile(analysisParams.getBamFile(), getStudy(), catalogManager.getFileManager(), token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        try {
            catalogBedFile = AnalysisUtils.getCatalogFile(analysisParams.getBedFile(), getStudy(), catalogManager.getFileManager(),
                    token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        try {
            catalogDictFile = AnalysisUtils.getCatalogFile(analysisParams.getDictFile(), getStudy(), catalogManager.getFileManager(),
                    token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

//        try {
//            catalogRefSeqFile = AnalysisUtils.getCatalogFile(analysisParams.getRefSeqFile(), getStudy(), catalogManager.getFileManager(),
//                    token);
//        } catch (CatalogException e) {
//            throw new ToolException(e);
//        }
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        steps.add(PICARD_BEDTOINTERVALLIST_STEP);
        steps.add(PICARD_COLLECTHSMETRICS_STEP);
        return steps;
    }

    @Override
    protected void run() throws ToolException {

        setUpStorageEngineExecutor(study);

        java.io.File baitFile = getOutDir().resolve("intervals.bait").toFile();
        java.io.File hsMetricsFile = getOutDir().resolve(HS_METRICS_FILENAME).toFile();

        step(PICARD_BEDTOINTERVALLIST_STEP, () -> {
            executorParams.put(EXECUTOR_ID, PicardWrapperAnalysisExecutor.ID);
            executorParams.put("I", catalogBedFile.getUri().getPath());
            executorParams.put("SD", catalogDictFile.getUri().getPath());
            executorParams.put("O", baitFile.getName());

            getToolExecutor(PicardWrapperAnalysisExecutor.class)
                    .setCommand("BedToIntervalList")
                    .execute();

            if (!baitFile.exists()) {
                throw new ToolException("Something wrong happened when running picard/BedToIntervalList. Please, check the stderr file for"
                        + " more details.");
            }
        });

        step(PICARD_COLLECTHSMETRICS_STEP, () -> {
            executorParams.put(EXECUTOR_ID, PicardWrapperAnalysisExecutor.ID);
            executorParams.put("I", catalogBamFile.getUri().getPath());
            executorParams.put("BI", baitFile.getAbsolutePath());
            executorParams.put("TI", baitFile.getAbsolutePath());
            executorParams.put("O", hsMetricsFile.getName());
            executorParams.remove("SD");

            getToolExecutor(PicardWrapperAnalysisExecutor.class)
                    .setCommand("CollectHsMetrics")
                    .execute();

            if (!hsMetricsFile.exists()) {
                throw new ToolException("Something wrong happened when running picard/CollectHsMetrics. Please, check the stderr file for"
                        + " more details.");
            }
        });
    }

    public static HsMetrics parseResults(Path outDir) throws ToolException {
        HsMetrics hsMetrics = null;
        try {
            // Parse HS metrics
            hsMetrics = HsMetricsParser.parse(outDir.resolve(HS_METRICS_FILENAME).toFile());
        } catch (IOException e) {
            new ToolException("Error parsing HS Metrics file: " + e.getMessage());
        }
        return hsMetrics;
    }
}
