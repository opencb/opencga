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
import org.opencb.opencga.core.models.file.FileRelatedFile;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.file.SmallRelatedFileParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

@Tool(id = AlignmentIndexOperation.ID, resource = Enums.Resource.ALIGNMENT, description = "Index alignment.")
public class AlignmentIndexOperation extends OpenCgaTool {

    public final static String ID = "alignment-index";
    public final static String DESCRIPTION = "Index a given alignment file, e.g., create a .bai file from a .bam file";

    private String study;
    private String inputFile;

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

        // Check if the index (.bai or .crai) has been already computed!
        if (filename.endsWith(".bam") && new java.io.File(inputPath + ".bai").exists()) {
            throw new ToolException("Index file '" + inputFile + ".bai' already exists");
        } else if (filename.endsWith(".cram") && new java.io.File(inputPath + ".crai").exists()) {
            throw new ToolException("Index file '" + inputFile + ".crai' already exists");
        }

        outputPath = getOutDir().resolve(filename + (filename.endsWith(".bam") ? ".bai" : ".crai"));
    }

    @Override
    protected void run() throws Exception {

        step(() -> {
            BamManager bamManager = new BamManager(inputPath);
            bamManager.createIndex(outputPath);
            bamManager.close();

            if (!outputPath.toFile().exists()) {
                throw new ToolException("Something wrong happened when computing index file for '" + inputFile + "'");
            } else {
                String catalogPath = Paths.get(new java.io.File(inputCatalogFile.getPath()).getParent()).toString();
                moveFile(study, outputPath, Paths.get(inputPath.toFile().getParent()),
                        catalogPath + "/" + outputPath.getFileName(), token);

                FileUpdateParams updateParams = new FileUpdateParams()
                        .setRelatedFiles(Collections.singletonList(new SmallRelatedFileParams(catalogPath + "/"
                                + outputPath.getFileName(), FileRelatedFile.Relation.PRODUCED_FROM)));
                catalogManager.getFileManager().update(study, inputFile, updateParams, QueryOptions.empty(), token);
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
}
