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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.wrappers.deeptools.DeeptoolsWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.CoverageIndexParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileInternalCoverageIndex;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.opencb.opencga.core.api.ParamConstants.COVERAGE_WINDOW_SIZE_DEFAULT;
import static org.opencb.opencga.core.tools.OpenCgaToolExecutor.EXECUTOR_ID;

@Tool(id = AlignmentCoverageAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = "Alignment coverage analysis.")
public class AlignmentCoverageAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "coverage-index-run";
    public static final String DESCRIPTION = "Compute the coverage from a given BAM alignment file, e.g., create a "
            + AlignmentConstants.BIGWIG_EXTENSION + " file from a " + AlignmentConstants.BAM_EXTENSION + " file";

    @ToolParams
    protected final CoverageIndexParams coverageParams = new CoverageIndexParams();

    private File bamCatalogFile;
    private File baiCatalogFile;

    @Override
    protected void check() throws Exception {
        super.check();

        // Sanity check
        if (StringUtils.isEmpty(getJobId())) {
            throw new ToolException("Missing job ID");
        }

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study when computing alignment coverage");
        }

        //  Checking BAM file ID
        try {
            bamCatalogFile = catalogManager.getFileManager().get(getStudy(), coverageParams.getFileId(), QueryOptions.empty(),
                    getToken()).first();
            if (bamCatalogFile == null) {
                throw new ToolException("Could not find BAM file from ID '" + coverageParams.getFileId() + "'");
            }
        } catch (Exception e) {
            throw new ToolException("Could not get BAM file from ID " + coverageParams.getFileId());
        }

        // Check if the input file is .bam
        if (!bamCatalogFile.getName().endsWith(AlignmentConstants.BAM_EXTENSION)) {
            throw new ToolException("Invalid input alignment file '" + coverageParams.getFileId() + "' (" + bamCatalogFile.getName()
                    + "): it must be in BAM format");
        }

        // Getting BAI file
        String baiFileId;
        try {
            baiFileId = bamCatalogFile.getInternal().getAlignment().getIndex().getFileId();
        } catch (Exception e) {
            throw new ToolException("Could not get internal alignment index file Id from BAM file ID '" + bamCatalogFile.getId() + "'");
        }
        if (StringUtils.isEmpty(baiFileId)) {
            throw new ToolException("Could not find the alignment index file for the BAM file ID '" + bamCatalogFile.getId() + "'. Please,"
                    + " create the alignment index file before computing the coverage");
        }
        try {
            baiCatalogFile = catalogManager.getFileManager().get(getStudy(), baiFileId, QueryOptions.empty(), getToken()).first();
            if (baiCatalogFile == null) {
                throw new ToolException("Could not find BAI file from ID '" + baiFileId + "'");
            }
        } catch (Exception e) {
            throw new ToolException("Could not get BAI file from file ID " + baiFileId);
        }

        logger.info("BAI file ID = {}; path = {}", baiCatalogFile.getId(), Paths.get(baiCatalogFile.getUri()));

        // Checking filenames
        if (!baiCatalogFile.getName().equals(bamCatalogFile.getName() + AlignmentConstants.BAI_EXTENSION)) {
            throw new ToolException("Filenames mismatch, BAI file name must consist of BAM file name plus the extension "
                    + AlignmentConstants.BAI_EXTENSION + "; BAM filename = " + bamCatalogFile.getName() + ", BAI filename = "
                    + baiCatalogFile.getName());
        }

        // Sanity check: window size
        logger.info("Checking window size {}", coverageParams.getWindowSize());
        if (coverageParams.getWindowSize() <= 0) {
            coverageParams.setWindowSize(Integer.parseInt(COVERAGE_WINDOW_SIZE_DEFAULT));
            logger.info("Window size is set to {}", coverageParams.getWindowSize());
        }

        // Check overwrite
        String bwFileId;
        try {
            bwFileId = bamCatalogFile.getInternal().getAlignment().getCoverage().getFileId();
        } catch (Exception e) {
            bwFileId = null;
        }
        if (StringUtils.isNotEmpty(bwFileId) && !coverageParams.isOverwrite()) {
            throw new ToolException("Coverage file ID '" + bwFileId + "' already exists for file ID '" + bamCatalogFile.getId()
                    + "'. To overwrite the BIGWIG file use the flag --overwrite");
        }
    }

    @Override
    protected void run() throws Exception {
        setUpStorageEngineExecutor(study);

        logger.info("Running with parameters {}", coverageParams);

        step(() -> {

            // Path where the BW file will be created
            Path bwPath = getOutDir().resolve(bamCatalogFile.getName() + AlignmentConstants.BIGWIG_EXTENSION);

            // In order to run "deeptools bamCoverage", both BAM and BAI files must be located in the same folder
            // Check if both BAM and BAI files are located in the same folder otherwise these files will symbolic-link temporarily
            // in the job dir to compute the BW file; then BAM and BAI symbolic links will be deleted from the job dir
            Path bamPath = Paths.get(bamCatalogFile.getUri()).toAbsolutePath();
            Path baiPath = Paths.get(baiCatalogFile.getUri()).toAbsolutePath();
            if (!bamPath.getParent().toString().equals(baiPath.getParent().toString())) {
                logger.info("BAM and BAI files must be symbolic-linked in the job dir since they are in different directories: {} and {}",
                        bamPath, baiPath);
                bamPath = getOutDir().resolve(bamCatalogFile.getName()).toAbsolutePath();
                baiPath = getOutDir().resolve(baiCatalogFile.getName()).toAbsolutePath();
                Files.createSymbolicLink(bamPath, Paths.get(bamCatalogFile.getUri()).toAbsolutePath());
                Files.createSymbolicLink(baiPath, Paths.get(baiCatalogFile.getUri()).toAbsolutePath());
            }

            Map<String, String> bamCoverageParams = new HashMap<>();
            bamCoverageParams.put("b", bamPath.toString());
            bamCoverageParams.put("o", bwPath.toAbsolutePath().toString());
            bamCoverageParams.put("binSize", String.valueOf(coverageParams.getWindowSize()));
            bamCoverageParams.put("outFileFormat", "bigwig");
            bamCoverageParams.put("minMappingQuality", "20");

            // Update executor parameters
            executorParams.appendAll(bamCoverageParams);
            executorParams.put(EXECUTOR_ID, DeeptoolsWrapperAnalysisExecutor.ID);

            getToolExecutor(DeeptoolsWrapperAnalysisExecutor.class)
                    .setStudy(getStudy())
                    .setCommand("bamCoverage")
                    .execute();

            // Remove symbolic links if necessary
            if (getOutDir().resolve(bamCatalogFile.getName()).toFile().exists()) {
                Files.delete(getOutDir().resolve(bamCatalogFile.getName()));
            }
            if (getOutDir().resolve(baiCatalogFile.getName()).toFile().exists()) {
                Files.delete(getOutDir().resolve(baiCatalogFile.getName()));
            }

            // Check execution result
            if (!bwPath.toFile().exists()) {
                throw new ToolException("Something wrong happened running a coverage: BigWig file (" + bwPath.toFile().getName()
                        + ") was not create, please, check log files.");
            }

            // Try to move the BW file into the BAM file directory
            boolean moveSuccessful = false;
            Path targetPath = Paths.get(bamCatalogFile.getUri()).getParent().resolve(bwPath.getFileName());
            try {
                Path movedPath = Files.move(bwPath, targetPath);
                moveSuccessful = targetPath.equals(movedPath);
            } catch (Exception e) {
                // Log message
                logger.info("Error moving the coverage file into the BAM folder {} to {}", bwPath, targetPath, e);
            }

            if (moveSuccessful) {
                bwPath = targetPath;
                logger.info("Coverage file was moved into the BAM folder: {}", bwPath);
            } else {
                logger.info("Couldn't move the coverage file into the BAM folder. The coverage file is in the job folder instead: {}",
                        bwPath);
            }

            // Link generated BIGWIG file and update samples info
            File bwCatalogFile = AlignmentAnalysisUtils.linkAndUpdate(bamCatalogFile, bwPath, getJobId(), study, catalogManager, token);

            // Update BAM file internal in order to set the coverage index (BIGWIG)
            FileInternalCoverageIndex fileCoverageIndex = new FileInternalCoverageIndex(new InternalStatus(InternalStatus.READY),
                    bwCatalogFile.getId(), "deeptools bamCoverage", coverageParams.getWindowSize());
            catalogManager.getFileManager().updateFileInternalCoverageIndex(study, bamCatalogFile, fileCoverageIndex, token);
        });
    }
}
