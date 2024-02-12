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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.wrappers.deeptools.DeeptoolsWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.CoverageIndexParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
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
    public final static String DESCRIPTION = "Compute the coverage from a given alignment file, e.g., create a .bw file from a .bam file";

    @ToolParams
    protected final CoverageIndexParams coverageParams = new CoverageIndexParams();

    private File bamCatalogFile;
    private File baiCatalogFile;

    private Path bwPath;

    protected void check() throws Exception {
        super.check();

        // Sanity check
        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study when computing alignment coverage");
        }

        //  Checking BAM file ID
        bamCatalogFile = getFile(coverageParams.getBamFileId(), "BAM");

        // Check if the input file is .bam
        if (!bamCatalogFile.getName().endsWith(".bam")) {
            throw new ToolException("Invalid input alignment file '" + coverageParams.getBamFileId() + "' (" + bamCatalogFile.getName()
                    + "): it must be in BAM format");
        }

        // Getting BAI file
        if (StringUtils.isEmpty(coverageParams.getBaiFileId())) {
            // BAI file ID was not provided, looking for it
            baiCatalogFile = getBaiFile(bamCatalogFile.getName() + ".bai");
        } else {
            // Getting the BAI file provided
            baiCatalogFile = getFile(coverageParams.getBaiFileId(), "BAI");
        }

        // Checking filenames
        if (!baiCatalogFile.getName().equals(bamCatalogFile.getName() + ".bai")) {
            throw new ToolException("Filenames mismatch, BAI file name consists of BAM file name plus the extension .bai; BAM filename = "
                    + bamCatalogFile.getName() + ", BAI filename = " + baiCatalogFile.getName());
        }

        // Sanity check: window size
        logger.info("{}: checking window size {}", ID, coverageParams.getWindowSize());
        if (coverageParams.getWindowSize() <= 0) {
            coverageParams.setWindowSize(Integer.parseInt(COVERAGE_WINDOW_SIZE_DEFAULT));
            logger.info("{}: window size is set to {}", ID, coverageParams.getWindowSize());
        }

        // Path where the BW file will be created
        bwPath = getOutDir().resolve(bamCatalogFile.getName() + ".bw");
    }

    @Override
    protected void run() throws Exception {
        setUpStorageEngineExecutor(study);

        logger.info("{}: running with parameters {}", ID, coverageParams);

        step(() -> {

            // In order to run "deeptools bamCoverage", both BAM and BAI files must be located in the same folder
            // to do that symbolic links will be created
            Files.createSymbolicLink(getOutDir().resolve(bamCatalogFile.getName()), Paths.get(bamCatalogFile.getUri()));
            Files.createSymbolicLink(getOutDir().resolve(baiCatalogFile.getName()), Paths.get(baiCatalogFile.getUri()));

            Map<String, String> bamCoverageParams = new HashMap<>();
            bamCoverageParams.put("b", Paths.get(bamCatalogFile.getUri()).toAbsolutePath().toString());
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

            // Remove symbolic links
            Files.delete(getOutDir().resolve(bamCatalogFile.getName()));
            Files.delete(getOutDir().resolve(baiCatalogFile.getName()));
        });
    }

    private File getFile(String fileId, String msg) throws ToolException {
        OpenCGAResult<File> fileResult;
        try {
            logger.info("{}: checking {} file ID '{}'", ID, msg, fileId);
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
            logger.info("{}: looking BAI file ID '{}'", ID, filename);
            fileResult = catalogManager.getFileManager().get(getStudy(), filename, QueryOptions.empty(), getToken());
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
                    if (Integer.parseInt(file.getCreationDate()) > Integer.parseInt(selectedFile.getCreationDate())) {
                        selectedFile = file;
                    }
                }
            }
            return selectedFile;
        }
    }
}
