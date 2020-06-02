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
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.nio.file.Path;
import java.nio.file.Paths;

@Tool(id = AlignmentIndexOperation.ID, resource = Enums.Resource.ALIGNMENT, description = "Index alignment.")
public class AlignmentIndexOperation extends OpenCgaTool {

    public final static String ID = "alignment-index-run";
    public final static String DESCRIPTION = "Index a given alignment file, e.g., create a .bai file from a .bam file";

    private String study;
    private String inputFile;
    private boolean overwrite;

    private File inputCatalogFile;
    private Path inputPath;
    private Path outputPath;

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
        if (!filename.endsWith(".bam") && !filename.endsWith(".cram")) {
            throw new ToolException("Invalid input alignment file '" + inputFile + "': it must be in BAM or CRAM format");
        }

        outputPath = getOutDir().resolve(filename + (filename.endsWith(".bam") ? ".bai" : ".crai"));
    }

    @Override
    protected void run() throws Exception {

        step(ID, () -> {

            Path indexPath = Paths.get(inputPath.toFile().getParent()).resolve(outputPath.getFileName());
            if (overwrite || !indexPath.toFile().exists()) {
                // Compute index if necessary
                BamManager bamManager = new BamManager(inputPath);
                bamManager.createIndex(outputPath);
                bamManager.close();

                if (!outputPath.toFile().exists()) {
                    throw new ToolException("Something wrong happened when computing index file for '" + inputFile + "'");
                }

                if (indexPath.toFile().exists()) {
                    indexPath.toFile().delete();
                }
                FileUtils.moveFile(outputPath.toFile(), indexPath.toFile());
            }

            boolean isLinked = true;
            Path outputCatalogPath = Paths.get(inputCatalogFile.getPath()).getParent().resolve(outputPath.getFileName());
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
                catalogManager.getFileManager().link(getStudy(), indexPath.toUri(), outputCatalogPath.getParent().toString(),
                        new ObjectMap("parents", true), token);
            }
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

    public boolean isOverwrite() {
        return overwrite;
    }

    public AlignmentIndexOperation setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }
}
