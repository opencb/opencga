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

import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Tool(id = AlignmentIndexOperation.ID, resource = Enums.Resource.ALIGNMENT, description = "Index alignment.")
public class AlignmentIndexOperation extends OpenCgaTool {

    public static final String ID = "alignment-index-run";
    public static final String DESCRIPTION = "Index a given alignment file, e.g., create a .bai file from a .bam file";

    private String study;
    private String inputFile;

    private File inputCatalogFile;
    private Path inputPath;
    private Path outputPath;

    @Override
    protected void check() throws Exception {
        super.check();

        OpenCGAResult<File> fileResult;
        try {
            fileResult = catalogManager.getFileManager().get(getStudy(), inputFile, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error accessing file '" + inputFile + "' of the study " + study + "'", e);
        }
        if (fileResult.getNumResults() <= 0) {
            throw new ToolException("File '" + inputFile + "' not found in study '" + study + "'");
        }

        inputCatalogFile = fileResult.getResults().get(0);
        inputPath = Paths.get(inputCatalogFile.getUri());
        String filename = inputPath.getFileName().toString();

        // Check if the input file is .bam or .cram
        if (!filename.endsWith(AlignmentConstants.BAM_EXTENSION) && !filename.endsWith(AlignmentConstants.CRAM_EXTENSION)) {
            throw new ToolException("Invalid input alignment file '" + inputFile + "': it must be in BAM or CRAM format");
        }

        outputPath = getOutDir().resolve(filename + (filename.endsWith(AlignmentConstants.BAM_EXTENSION)
                ? AlignmentConstants.BAI_EXTENSION : AlignmentConstants.CRAI_EXTENSION));
    }

    @Override
    protected void run() throws Exception {

        step(ID, () -> {
            // Compute index if necessary
            logger.info("Computing alignment index for {}", inputPath);
            BamManager bamManager = new BamManager(inputPath);
            bamManager.createIndex(outputPath);
            bamManager.close();

            if (!outputPath.toFile().exists()) {
                throw new ToolException("Something wrong happened when computing index file for '" + inputFile + "'");
            }

            // Try to copy the BAI file into the BAM file directory
            Path targetPath = inputPath.getParent().resolve(outputPath.getFileName());
            try {
                Files.move(outputPath, targetPath);
            } catch (Exception e) {
                // Do nothing
                logger.info("Moving from {} to {}: {}", outputPath, targetPath, e.getMessage());
            }

            if (targetPath.toFile().exists()) {
                outputPath = targetPath;
                logger.info("Alignment index file was copied into the BAM folder: {}", outputPath);
            } else {
                logger.info("Couldn't copy the alignment index file into the BAM folder. The index file is in the job folder instead: {}",
                        outputPath);
            }

            // Link generated BAI file and update samples info, related file
            File baiCatalogFile = AlignmentAnalysisUtils.linkAndUpdate(inputCatalogFile, outputPath, getJobId(), study, catalogManager, token);

            // Update BAM file internal in order to set the alignment index (BAI)
            FileInternalAlignmentIndex fileAlignmentIndex = new FileInternalAlignmentIndex(new InternalStatus(InternalStatus.READY),
                    baiCatalogFile.getId(), "HTSJDK library");
            catalogManager.getFileManager().updateFileInternalAlignmentIndex(inputCatalogFile, fileAlignmentIndex, token);
        });
    }

    public String getStudy() {
        return study;
    }

    public AlignmentIndexOperation setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getInputFile() {
        return inputFile;
    }

    public AlignmentIndexOperation setInputFile(String inputFile) {
        this.inputFile = inputFile;
        return this;
    }
}
