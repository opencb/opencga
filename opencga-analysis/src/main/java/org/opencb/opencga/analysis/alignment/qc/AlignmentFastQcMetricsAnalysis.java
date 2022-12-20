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
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.sequence.fastqc.FastQcMetrics;
import org.opencb.biodata.formats.sequence.fastqc.io.FastQcParser;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.wrappers.fastqc.FastqcWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.AlignmentFastQcMetricsParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileQualityControl;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.ALIGNMENT_FASTQC_METRICS_DESCRIPTION;
import static org.opencb.opencga.core.tools.OpenCgaToolExecutor.EXECUTOR_ID;

@Tool(id = AlignmentFastQcMetricsAnalysis.ID, resource = Enums.Resource.ALIGNMENT)
public class AlignmentFastQcMetricsAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "alignment-fastqcmetrics";
    public static final String DESCRIPTION = ALIGNMENT_FASTQC_METRICS_DESCRIPTION;

    private static final String FASTQC_STEP = "fastqc";

    @ToolParams
    protected final AlignmentFastQcMetricsParams analysisParams = new AlignmentFastQcMetricsParams();

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
        steps.add(FASTQC_STEP);
        return steps;
    }

    @Override
    protected void run() throws ToolException {

        setUpStorageEngineExecutor(study);

        step(FASTQC_STEP, () -> {
            executorParams.put(EXECUTOR_ID, FastqcWrapperAnalysisExecutor.ID);
            executorParams.put("extract", "true");
            getToolExecutor(FastqcWrapperAnalysisExecutor.class)
                    .setInputFile(catalogBamFile.getUri().getPath())
                    .execute();
        });
    }

    public static FastQcMetrics parseResults(Path outDir) throws ToolException {
        Path fastQcPath = null;
        Path imgPath = null;
        for (java.io.File file : outDir.toFile().listFiles()) {
            if (file.isDirectory() && file.getName().endsWith("_fastqc")) {
                fastQcPath = file.toPath().resolve("fastqc_data.txt");
                imgPath = file.toPath().resolve("Images");
            }
        }

        FastQcMetrics fastQcMetrics = null;
        try {
            if (fastQcPath != null && fastQcPath.toFile().exists()) {

                fastQcMetrics = FastQcParser.parse(fastQcPath.toFile());
                FastQcParser.addImages(imgPath, fastQcMetrics);

                // Replace absolute paths to relative paths
                List<String> relativePaths = new ArrayList<>();
                for (String path : fastQcMetrics.getFiles()) {
                    int index = path.indexOf("JOBS/");
                    relativePaths.add(index == -1 ? new java.io.File(path).getName() : path.substring(index));
                }
                fastQcMetrics.setFiles(relativePaths);
            } else {
                throw new ToolException("Something wrong happened: FastQC file " + fastQcPath.getFileName() + " not found!");
            }
        } catch (IOException e) {
            new ToolException("Error parsing Alignment FastQC Metrics file: " + e.getMessage());
        }

        return fastQcMetrics;
    }
}