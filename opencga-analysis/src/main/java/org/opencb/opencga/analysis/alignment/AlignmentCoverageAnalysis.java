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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.wrappers.deeptools.DeeptoolsWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.CoverageIndexParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
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

    public final static String ID = "coverage-index-run";
    public final static String DESCRIPTION = "Compute the coverage from a given alignment file, e.g., create a "
            + AlignmentConstants.BIGWIG_EXTENSION + " file from a " + AlignmentConstants.BAM_EXTENSION + " file";

    @ToolParams
    protected final CoverageIndexParams coverageParams = new CoverageIndexParams();

    private File bamCatalogFile;
    private File baiCatalogFile;

    protected void check() throws Exception {
        super.check();

        // Sanity check
        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study when computing alignment coverage");
        }

        //  Checking BAM file ID
        bamCatalogFile = getFile(coverageParams.getBamFileId(), "BAM");

        // Check if the input file is .bam
        if (!bamCatalogFile.getName().endsWith(AlignmentConstants.BAM_EXTENSION)) {
            throw new ToolException("Invalid input alignment file '" + coverageParams.getBamFileId() + "' (" + bamCatalogFile.getName()
                    + "): it must be in BAM format");
        }

        // Getting BAI file
        if (StringUtils.isEmpty(coverageParams.getBaiFileId())) {
            // BAI file ID was not provided, looking for it
            baiCatalogFile = getBaiFile(bamCatalogFile.getName() + AlignmentConstants.BAI_EXTENSION);
        } else {
            // Getting the BAI file provided
            baiCatalogFile = getFile(coverageParams.getBaiFileId(), "BAI");
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
            if (bamPath.getParent() != baiPath.getParent()) {
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

            // Check execution result
            if (!bwPath.toFile().exists()) {
                new ToolException("Something wrong happened running a coverage: BigWig file (" + bwPath.toFile().getName()
                        + ") was not create, please, check log files.");
            }

            // Link BW file and update sample info
            FileLinkParams fileLinkParams = new FileLinkParams().setUri(bwPath.toString());
            if (Paths.get(bamCatalogFile.getPath()).getParent() != null) {
                fileLinkParams.setPath(Paths.get(bamCatalogFile.getPath()).getParent().resolve(bwPath.getFileName()).toString());
            }
            OpenCGAResult<File> fileResult = catalogManager.getFileManager().link(study, fileLinkParams, false, token);
            if (fileResult.getNumResults() != 1) {
                throw new ToolException("It could not link OpenCGA BAI file catalog file for '" + coverageParams.getBamFileId() + "'");
            }
            FileUpdateParams updateParams = new FileUpdateParams().setSampleIds(bamCatalogFile.getSampleIds());
            catalogManager.getFileManager().update(study, fileResult.first().getId(), updateParams, null, token);
            fileResult = catalogManager.getFileManager().get(study, fileResult.first().getId(), QueryOptions.empty(), token);
            if (!fileResult.first().getSampleIds().equals(bamCatalogFile.getSampleIds())) {
                throw new ToolException("It could not update sample IDS within the OpenCGA BAI file catalog (" + fileResult.first().getId()
                        + ") with the samples info from '" + coverageParams.getBamFileId() + "'");
            }

            // Remove symbolic links if necessary
            if (getOutDir().resolve(bamCatalogFile.getName()).toFile().exists()) {
                Files.delete(getOutDir().resolve(bamCatalogFile.getName()));
            }
            if (getOutDir().resolve(baiCatalogFile.getName()).toFile().exists()) {
                Files.delete(getOutDir().resolve(baiCatalogFile.getName()));
            }
        });
    }

    private File getFile(String fileId, String msg) throws ToolException {
        OpenCGAResult<File> fileResult;
        try {
            logger.info("Checking {} file ID '{}'", msg, fileId);
            fileResult = catalogManager.getFileManager().get(getStudy(), fileId, QueryOptions.empty(), getToken());
        } catch (CatalogException e) {
            throw new ToolException("Error accessing " + msg + " file '" + fileId + "' of the study " + getStudy() + "'", e);
        }
        if (fileResult.getNumResults() <= 0) {
            throw new ToolException(msg + " file ID '" + fileId + "' not found in study '" + getStudy() + "'");
        }
        return  fileResult.first();
    }

    private File getBaiFile(String filename) throws ToolException {
        OpenCGAResult<File> fileResult;
        try {
            logger.info("Looking for BAI file ID from name '{}'", filename);
            Query query = new Query(FileDBAdaptor.QueryParams.NAME.key(), filename);
            fileResult = catalogManager.getFileManager().search(getStudy(), query, QueryOptions.empty(), getToken());
        } catch (CatalogException e) {
            throw new ToolException("Error accessing BAI file name '" + filename + "' of the study " + getStudy() + "'", e);
        }
        if (fileResult.getNumResults() <= 0) {
            throw new ToolException("Filename '" + filename + "' not found in study '" + getStudy() + "'");
        } else {
            File selectedFile = null;
            for (File file : fileResult.getResults()) {
                if (selectedFile == null) {
                    selectedFile = file;
                } else {
                    // Get the most recent according to the creation date
                    // Creation date keeps the format: 20240212151427
                    if (file.getCreationDate().compareTo(selectedFile.getCreationDate()) > 0) {
                        selectedFile = file;
                    }
                }
            }
            return selectedFile;
        }
    }
}
