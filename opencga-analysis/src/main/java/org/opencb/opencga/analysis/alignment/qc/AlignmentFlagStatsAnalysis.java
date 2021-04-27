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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.alignment.samtools.io.SamtoolsFlagstatsParser;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.samtools.SamtoolsWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.AlignmentFlagStatsParams;
import org.opencb.opencga.core.models.alignment.AlignmentStatsParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileQualityControl;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.file.PostLinkToolParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.commons.io.FileUtils.readLines;
import static org.opencb.opencga.core.api.ParamConstants.ALIGNMENT_FLAG_STATS_DESCRIPTION;
import static org.opencb.opencga.core.tools.OpenCgaToolExecutor.EXECUTOR_ID;

@Tool(id = AlignmentFlagStatsAnalysis.ID, resource = Enums.Resource.ALIGNMENT)
public class AlignmentFlagStatsAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "alignment-flagstats";
    public static final String DESCRIPTION = ALIGNMENT_FLAG_STATS_DESCRIPTION;

    private static final String SAMTOOLS_FLAG_STATS_STEP = "samtools-flagstat";
    private static final String SAVE_ALIGNMENT_FLAG_STATS_STEP = "save-alignment-flagstats";

    @ToolParams
    protected final AlignmentFlagStatsParams analysisParams = new AlignmentFlagStatsParams();

    private File catalogBamFile;

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        if (StringUtils.isEmpty(analysisParams.getFile())) {
            throw new ToolException("Missing file");
        }

        Query query = new Query(FileDBAdaptor.QueryParams.ID.key(), analysisParams.getFile());
        query.put(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.BAM);
        OpenCGAResult<File> fileResult = catalogManager.getFileManager().search(getStudy(), query, QueryOptions.empty(), token);
        if (fileResult.getNumResults() != 1) {
            throw new ToolException("File " + analysisParams.getFile() + " must be a BAM file in study " + getStudy());
        }

        catalogBamFile = fileResult.getResults().get(0);
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        steps.add(SAMTOOLS_FLAG_STATS_STEP);
        steps.add(SAVE_ALIGNMENT_FLAG_STATS_STEP);
        return steps;
    }

    @Override
    protected void run() throws ToolException {
        
        setUpStorageEngineExecutor(study);

        Path flagStatsFile = getOutDir().resolve(new java.io.File(catalogBamFile.getUri().getPath()).getName() + ".flagstats.txt");

        step(SAMTOOLS_FLAG_STATS_STEP, () -> {
            executorParams.put(EXECUTOR_ID, SamtoolsWrapperAnalysisExecutor.ID);
            getToolExecutor(SamtoolsWrapperAnalysisExecutor.class)
                    .setCommand("flagstat")
                    .setInputFile(catalogBamFile.getUri().getPath())
                    .execute();

            // Check results
            java.io.File stdoutFile = getOutDir().resolve(DockerWrapperAnalysisExecutor.STDOUT_FILENAME).toFile();
            List<String> lines = readLines(stdoutFile, Charset.defaultCharset());
            if (lines.size() > 0 && lines.get(0).contains("QC-passed")) {
                FileUtils.copyFile(stdoutFile, flagStatsFile.toFile());
            } else {
                throw new ToolException("Something wrong happened running samtools-flagstat.");
            }
        });

        step(SAVE_ALIGNMENT_FLAG_STATS_STEP, () -> {

            SamtoolsFlagstats flagStats = SamtoolsFlagstatsParser.parse(flagStatsFile);

            // Update quality control for the catalog file
            FileQualityControl qc = catalogBamFile.getQualityControl();
            // Sanity check
            if (qc == null) {
                qc = new FileQualityControl();
            }
            qc.getAlignmentQualityControl().setSamtoolsFlagStats(flagStats);

            catalogManager.getFileManager().update(getStudy(), catalogBamFile.getId(), new FileUpdateParams().setQualityControl(qc),
                    QueryOptions.empty(), getToken());
        });
    }
}
