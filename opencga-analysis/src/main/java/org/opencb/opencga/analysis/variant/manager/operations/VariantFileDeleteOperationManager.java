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

package org.opencb.opencga.analysis.variant.manager.operations;

import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileIndex;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.util.ArrayList;
import java.util.List;

public class VariantFileDeleteOperationManager extends OperationManager {

    public VariantFileDeleteOperationManager(VariantStorageManager variantStorageManager, VariantStorageEngine engine) {
        super(variantStorageManager, engine);
    }

    public void removeStudy(String study, String token) throws CatalogException, StorageEngineException {
        study = getStudyFqn(study, token);

        // Update study configuration BEFORE executing the operation and fetching files from Catalog
        synchronizeMetadata(study, token, null);

        variantStorageEngine.removeStudy(study);

        new CatalogStorageMetadataSynchronizer(catalogManager, variantStorageEngine.getMetadataManager())
                .synchronizeRemovedStudyFromStorage(study, token);

    }

    public void removeFile(String study, List<String> files, String token) throws CatalogException, StorageEngineException {
        // Update study configuration BEFORE executing the operation and fetching files from Catalog
        files = synchronizeMetadata(study, token, files);

        variantStorageEngine.removeFiles(study, files);
        // Update study configuration to synchronize
        synchronizeCatalogStudyFromStorage(study, token);

    }

    private List<String> synchronizeMetadata(String study, String token, List<String> files)
            throws CatalogException, StorageEngineException {
        StudyMetadata studyMetadata = synchronizeCatalogStudyFromStorage(study, token);
        if (studyMetadata == null) {
            throw new CatalogException("Study '" + study + "' does not exist on the VariantStorage");
        }
        List<String> fileNames = new ArrayList<>();
        if (files != null && !files.isEmpty()) {
            for (String fileStr : files) {
                File file = catalogManager.getFileManager().get(study, fileStr, null, token).first();
                String catalogIndexStatus = file.getInternal().getIndex().getStatus().getName();
                if (!catalogIndexStatus.equals(FileIndex.IndexStatus.READY)) {
                    // Might be partially loaded in VariantStorage. Check FileMetadata
                    FileMetadata fileMetadata = variantStorageEngine.getMetadataManager().getFileMetadata(studyMetadata.getId(), fileStr);
                    if (fileMetadata == null || !fileMetadata.getIndexStatus().equals(TaskMetadata.Status.NONE)) {
                        throw new CatalogException("Unable to remove variants from file " + file.getName() + ". "
                                + "IndexStatus = " + catalogIndexStatus);
                    }
                }
                fileNames.add(file.getName());
//                        filePaths.add(file.getPath());
            }

            if (fileNames.isEmpty()) {
                throw new CatalogException("Nothing to do!");
            }
        }
        return fileNames;
    }

}
