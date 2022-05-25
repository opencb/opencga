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
import org.opencb.opencga.core.models.file.VariantIndexStatus;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class VariantDeleteOperationManager extends OperationManager {

    public VariantDeleteOperationManager(VariantStorageManager variantStorageManager, VariantStorageEngine engine) {
        super(variantStorageManager, engine);
    }

    public void removeStudy(String study, URI outdir, String token) throws CatalogException, StorageEngineException {
        study = getStudyFqn(study, token);

        // Update study configuration BEFORE executing the operation and fetching files from Catalog
        synchronizeCatalogStudyFromStorage(study, token, true);

        variantStorageEngine.removeStudy(study, outdir);

        new CatalogStorageMetadataSynchronizer(catalogManager, variantStorageEngine.getMetadataManager())
                .synchronizeRemovedStudyFromStorage(study, token);

    }

    public void removeFile(String study, List<String> inputFiles, URI outdir, String token) throws CatalogException, StorageEngineException {
        // Update study metadata BEFORE executing the operation and fetching files from Catalog
        StudyMetadata studyMetadata = synchronizeCatalogStudyFromStorage(study, token, true);

        List<String> fileNames = new ArrayList<>();
        if (inputFiles != null && !inputFiles.isEmpty()) {
            for (String fileStr : inputFiles) {
                File file = catalogManager.getFileManager().get(study, fileStr, null, token).first();
                String catalogIndexStatus = file.getInternal().getVariant().getIndex().getStatus().getId();
                if (!catalogIndexStatus.equals(VariantIndexStatus.READY)) {
                    // Might be partially loaded in VariantStorage. Check FileMetadata
                    FileMetadata fileMetadata = variantStorageEngine.getMetadataManager().getFileMetadata(studyMetadata.getId(), fileStr);
                    if (fileMetadata == null || fileMetadata.getIndexStatus() != TaskMetadata.Status.NONE) {
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

        variantStorageEngine.removeFiles(study, fileNames, outdir);
        // Update study configuration to synchronize
        synchronizeCatalogStudyFromStorage(study, token, true);

    }

    public void removeSample(String study, List<String> samples, URI outdir, String token) throws CatalogException, StorageEngineException {
        // Update study metadata BEFORE executing the operation and fetching files from Catalog
        synchronizeCatalogStudyFromStorage(study, token, true);

//        Set<String> files = new HashSet<>();
//        for (Sample sample : catalogManager.getSampleManager().get(study, samples,
//                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.FILE_IDS.key()), token).getResults()) {
//            files.addAll(sample.getFileIds());
//        }

        variantStorageEngine.removeSamples(study, samples, outdir);
        // Update study configuration to synchronize
        synchronizeCatalogStudyFromStorage(study, token, true);

    }


}
