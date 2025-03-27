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

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

public class AlignmentAnalysisUtils {

    public static File linkAndUpdate(File bamCatalogFile, Path outPath, String jobId, String study, CatalogManager catalogManager,
                                     String token) throws CatalogException, ToolException {
        Path catalogOutPath;
        if (jobId == null) {
            if (Paths.get(bamCatalogFile.getPath()).getParent() == null) {
                catalogOutPath = outPath.getFileName();
            } else {
                catalogOutPath = Paths.get(bamCatalogFile.getPath()).resolve(outPath.getFileName());
            }
        } else {
            Job job = catalogManager.getJobManager().get(study, jobId, QueryOptions.empty(), token).first();
            catalogOutPath = Paths.get(job.getOutDir().getPath()).resolve(outPath.getFileName());
        }

        // Link BAI/BIGWIG file and update sample info; outPath is BAI or BIGWIG file
        FileLinkParams fileLinkParams = new FileLinkParams()
                .setUri(outPath.toString())
                .setPath(catalogOutPath.toString());
        OpenCGAResult<File> fileResult = catalogManager.getFileManager().link(study, fileLinkParams, true, token);
        if (fileResult.getNumResults() != 1) {
            throw new ToolException("It could not link OpenCGA file catalog file for '" + outPath + "'");
        }
        File outCatalogFile = fileResult.first();

        // Updating file: samples, related file
        FileUpdateParams updateParams = new FileUpdateParams()
                .setSampleIds(bamCatalogFile.getSampleIds())
                .setRelatedFiles(Collections.singletonList(new SmallRelatedFileParams()
                        .setFile(bamCatalogFile.getId())
                        .setRelation(FileRelatedFile.Relation.ALIGNMENT)));
        try {
            OpenCGAResult<File> updateResult = catalogManager.getFileManager().update(study, outCatalogFile.getId(), updateParams, null,
                    token);
            if (updateResult.getNumUpdated() != 1) {
                catalogManager.getFileManager().unlink(study, outCatalogFile.getId(), token);
                throw new ToolException("It could not update OpenCGA file catalog (" + outCatalogFile.getId()
                        + ") from alignment file ID '" + bamCatalogFile.getId() + "'");
            }
        } catch (CatalogException e) {
            catalogManager.getFileManager().unlink(study, outCatalogFile.getId(), token);
            throw e;
        }
        return outCatalogFile;
    }
}
