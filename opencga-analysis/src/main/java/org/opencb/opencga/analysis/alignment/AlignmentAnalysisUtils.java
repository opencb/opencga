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
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.nio.file.Path;
import java.nio.file.Paths;

public class AlignmentAnalysisUtils {

    public static void linkAndUpdate(File bamCatalogFile, Path outPath, String study, CatalogManager catalogManager, String token)
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
        FileUpdateParams updateParams = new FileUpdateParams().setSampleIds(bamCatalogFile.getSampleIds());
        catalogManager.getFileManager().update(study, fileResult.first().getId(), updateParams, null, token);
        fileResult = catalogManager.getFileManager().get(study, fileResult.first().getId(), QueryOptions.empty(), token);
        if (!fileResult.first().getSampleIds().equals(bamCatalogFile.getSampleIds())) {
            throw new ToolException("It could not update sample IDS within the OpenCGA BAI file catalog (" + fileResult.first().getId()
                    + ") with the samples info from '" + bamCatalogFile.getId() + "'");
        }
    }

}
