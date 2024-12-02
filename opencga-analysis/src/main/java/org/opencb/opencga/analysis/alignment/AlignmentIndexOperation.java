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

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.AlignmentIndexParams;
import org.opencb.opencga.core.models.alignment.CoverageIndexParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Tool(id = AlignmentIndexOperation.ID, resource = Enums.Resource.ALIGNMENT, description = "Index alignment.")
public class AlignmentIndexOperation extends OpenCgaToolScopeStudy {

    public static final String ID = "alignment-index-run";
    public static final String DESCRIPTION = "Index a given alignment file, e.g., create a .bai file from a .bam file";

    @ToolParams
    protected final AlignmentIndexParams indexParams = new AlignmentIndexParams();

    private File inputCatalogFile;
    private Path inputPath;
    private Path outputPath;

    @Override
    protected void check() throws Exception {
        super.check();

        // Sanity check
        if (StringUtils.isEmpty(getJobId())) {
            throw new ToolException("Missing job ID");
        }

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study when computing alignment index");
        }

        OpenCGAResult<File> fileResult;
        try {
            fileResult = catalogManager.getFileManager().get(getStudy(), indexParams.getFileId(), QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error accessing file '" + indexParams.getFileId() + "' of the study " + study + "'", e);
        }
        if (fileResult.getNumResults() <= 0) {
            throw new ToolException("File '" + indexParams.getFileId() + "' not found in study '" + study + "'");
        }

        inputCatalogFile = fileResult.getResults().get(0);
        inputPath = Paths.get(inputCatalogFile.getUri());
        String filename = inputPath.getFileName().toString();

        // Check if the input file is .bam or .cram
        if (!filename.endsWith(AlignmentConstants.BAM_EXTENSION) && !filename.endsWith(AlignmentConstants.CRAM_EXTENSION)) {
            throw new ToolException("Invalid input alignment file '" + indexParams.getFileId() + "': it must be in BAM or CRAM format");
        }

        outputPath = getOutDir().resolve(filename + (filename.endsWith(AlignmentConstants.BAM_EXTENSION)
                ? AlignmentConstants.BAI_EXTENSION : AlignmentConstants.CRAI_EXTENSION));
    }

    @Override
    protected void run() throws Exception {
        setUpStorageEngineExecutor(study);

        logger.info("Running with parameters {}", indexParams);

        step(ID, () -> {
            // Compute index if necessary
            logger.info("Computing alignment index for {}", inputPath);
            BamManager bamManager = new BamManager(inputPath);
            bamManager.createIndex(outputPath);
            bamManager.close();

            if (!outputPath.toFile().exists()) {
                throw new ToolException("Something wrong happened when computing index file for '" + indexParams.getFileId() + "'");
            }

            // Try to move the BAI file into the BAM file directory
            boolean moveSuccessful = false;
            Path targetPath = inputPath.getParent().resolve(outputPath.getFileName());
            try {
                Path movedPath = Files.move(outputPath, targetPath);
                moveSuccessful = targetPath.equals(movedPath);
            } catch (Exception e) {
                // Log message
                logger.info("Error moving from {} to {}", outputPath, targetPath, e);
            }

            if (moveSuccessful) {
                outputPath = targetPath;
                logger.info("Alignment index file was moved into the BAM folder: {}", outputPath);
            } else {
                logger.info("Couldn't move the alignment index file into the BAM folder. The index file is in the job folder instead: {}",
                        outputPath);
            }

            // Link generated BAI file and update samples info, related file
            File baiCatalogFile = AlignmentAnalysisUtils.linkAndUpdate(inputCatalogFile, outputPath, getJobId(), study, catalogManager,
                    token);

            // Update BAM file internal in order to set the alignment index (BAI)
            FileInternalAlignmentIndex fileAlignmentIndex = new FileInternalAlignmentIndex(new InternalStatus(InternalStatus.READY),
                    baiCatalogFile.getId(), "HTSJDK library");
            catalogManager.getFileManager().updateFileInternalAlignmentIndex(inputCatalogFile, fileAlignmentIndex, token);
        });
    }
}
