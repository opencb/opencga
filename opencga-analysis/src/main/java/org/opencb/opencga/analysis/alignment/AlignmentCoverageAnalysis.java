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

package org.opencb.opencga.analysis.alignment;

import org.apache.commons.io.FileUtils;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.analysis.wrappers.deeptools.DeeptoolsWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.CoverageIndexParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.opencb.opencga.core.api.ParamConstants.COVERAGE_WINDOW_SIZE_DEFAULT;
import static org.opencb.opencga.core.tools.OpenCgaToolExecutor.EXECUTOR_ID;

@Tool(id = AlignmentCoverageAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = "Alignment coverage analysis.")
public class AlignmentCoverageAnalysis extends OpenCgaTool {

    public final static String ID = "coverage-index-run";
    public final static String DESCRIPTION = "Compute the coverage from a given alignment file, e.g., create a .bw file from a .bam file";

    @ToolParams
    protected final CoverageIndexParams coverageIndexParams = new CoverageIndexParams();

    private String study;
    private String inputFile;
    private boolean overwrite;

    private File bamCatalogFile;
    private Path inputPath;

    private Path bwCatalogPath;
    private Path outputPath;

    protected void check() throws Exception {
        super.check();

        OpenCGAResult<File> fileResult;
        try {
            logger.info("CoverageIndexAnalysis: checking file {}", coverageIndexParams.getFile());
            fileResult = catalogManager.getFileManager().get(getStudy(), coverageIndexParams.getFile(), QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error accessing file '" + inputFile + "' of the study " + study + "'", e);
        }
        if (fileResult.getNumResults() <= 0) {
            throw new ToolException("File '" + inputFile + "' not found in study '" + study + "'");
        }

        bamCatalogFile = fileResult.getResults().get(0);
        inputPath = Paths.get(bamCatalogFile.getUri());
        String filename = inputPath.getFileName().toString();

        // Check if the input file is .bam or .cram
        if (!filename.endsWith(".bam")) {
            throw new ToolException("Invalid input alignment file '" + inputFile + "': it must be in BAM format");
        }

        // Sanity check: window size
        if (coverageIndexParams.getWindowSize() <= 0) {
            coverageIndexParams.setWindowSize(Integer.parseInt(COVERAGE_WINDOW_SIZE_DEFAULT));
        }

        // Path where the BW file will be created
        outputPath = getOutDir().resolve(filename + ".bw");

        // Check if BW exists already, and then check the flag 'overwrite'
        bwCatalogPath = Paths.get(inputPath.toFile().getParent()).resolve(outputPath.getFileName());
        if (bwCatalogPath.toFile().exists() && !coverageIndexParams.isOverwrite()) {
            // Nothing to do
            throw new ToolException("Nothing to do: coverage file (" + bwCatalogPath + ") already exists and you set the flag 'overwrite'"
                    + " to false");
        }
    }

    @Override
    protected void run() throws Exception {
        setUpStorageEngineExecutor(study);

        step(() -> {
            Map<String, String> bamCoverageParams = new HashMap<>();
            bamCoverageParams.put("b", inputPath.toAbsolutePath().toString());
            bamCoverageParams.put("o", outputPath.toAbsolutePath().toString());
            bamCoverageParams.put("binSize", String.valueOf(coverageIndexParams.getWindowSize()));
            bamCoverageParams.put("outFileFormat", "bigwig");
            bamCoverageParams.put("minMappingQuality", "20");

            // Update executor parameters
            executorParams.appendAll(bamCoverageParams);
            executorParams.put(EXECUTOR_ID, DeeptoolsWrapperAnalysisExecutor.ID);

            getToolExecutor(DeeptoolsWrapperAnalysisExecutor.class)
                    .setStudy(study)
                    .setCommand("bamCoverage")
                    .execute();

            // Check execution result
            if (!outputPath.toFile().exists()) {
                new ToolException("Something wrong happened running a coverage: BigWig file (" + outputPath.toFile().getName()
                        + ") was not create, please, check log files.");
            }

            // Move the BW file to the same directory where the BAM file is located
            if (bwCatalogPath.toFile().exists()) {
                bwCatalogPath.toFile().delete();
            }
            FileUtils.moveFile(outputPath.toFile(), bwCatalogPath.toFile());

            // And finally, link the BW file is necessary
            boolean isLinked = true;
            Path outputCatalogPath = Paths.get(bamCatalogFile.getPath()).getParent().resolve(outputPath.getFileName());
            OpenCGAResult<File> fileResult;
            try {
                fileResult = catalogManager.getFileManager().get(getStudy(), outputCatalogPath.toString(), QueryOptions.empty(), token);
                if (fileResult.getNumResults() <= 0) {
                    isLinked = false;
                }
            } catch (CatalogException e) {
                isLinked = false;
            }
            if (!isLinked) {
                catalogManager.getFileManager().link(getStudy(), bwCatalogPath.toUri(), outputCatalogPath.getParent().toString(),
                        new ObjectMap("parents", true), token);
            }
        });
    }

    public String getStudy() {
        return study;
    }

    public AlignmentCoverageAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getInputFile() {
        return inputFile;
    }

    public AlignmentCoverageAnalysis setInputFile(String inputFile) {
        this.inputFile = inputFile;
        return this;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public AlignmentCoverageAnalysis setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }
}
