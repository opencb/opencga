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

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

public class AlignmentAnalysisUtils {

    public static File linkAndUpdate(File bamCatalogFile, Path outPath, String study, CatalogManager catalogManager, String token)
            throws CatalogException, ToolException {
        // Link BW file and update sample info
        FileLinkParams fileLinkParams = new FileLinkParams().setUri(outPath.toString());
        if (Paths.get(bamCatalogFile.getPath()).getParent() != null) {
            fileLinkParams.setPath(Paths.get(bamCatalogFile.getPath()).getParent().resolve(outPath.getFileName()).toString());
        }
        OpenCGAResult<File> fileResult = catalogManager.getFileManager().link(study, fileLinkParams, false, token);
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
