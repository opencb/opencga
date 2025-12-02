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

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.VariantIndexStatus;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class VariantDeleteOperationManager extends OperationManager {

    private final Logger logger = LoggerFactory.getLogger(VariantDeleteOperationManager.class);

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
        boolean force = variantStorageEngine.getOptions().getBoolean(VariantStorageOptions.FORCE.key());
        StudyMetadata studyMetadata = synchronizeCatalogStudyFromStorage(study, token, true);

        List<String> fileNames = new ArrayList<>();
        if (inputFiles != null && !inputFiles.isEmpty()) {
            for (String fileStr : inputFiles) {
                File file = catalogManager.getFileManager().get(study, fileStr, null, token).first();
                String catalogIndexStatus = file.getInternal().getVariant().getIndex().getStatus().getId();
                if (!catalogIndexStatus.equals(VariantIndexStatus.READY)) {
                    // Might be partially loaded in VariantStorage. Check FileMetadata
                    FileMetadata fileMetadata = variantStorageEngine.getMetadataManager()
                            .getFileMetadata(studyMetadata.getId(), file.getName());
                    if (fileMetadata != null && !fileMetadata.getPath().equals(file.getUri().getPath())) {
                        // FileMetadata path does not match the catalog path. This file is not registered in the storage.
                        throw new CatalogException("Unable to remove variants from file '" + file.getPath() + "'. "
                                + "File is not registered in the storage. "
                                + "Instead, found file with same name but different path '" + fileMetadata.getPath() + "'");
                    }
                    boolean canBeRemoved;
                    if (force) {
                        // When forcing remove, just require the file to be registered in the storage
                        canBeRemoved = fileMetadata != null;
                    } else {
                        // Otherwise, require the file to be in status NONE
                        canBeRemoved = fileMetadata != null && fileMetadata.getIndexStatus() != TaskMetadata.Status.NONE;
                    }
                    if (!canBeRemoved) {
                        throw new CatalogException("Unable to remove variants from file '" + file.getPath() + "'. "
                                + "IndexStatus = " + catalogIndexStatus + "."
                                + (fileMetadata == null ? " File not found in storage." : ""));
                    }
                }
                fileNames.add(file.getName());
//                        filePaths.add(file.getPath());
            }

        }
        if (fileNames.isEmpty()) {
            throw new CatalogException("Nothing to do!");
        }

        variantStorageEngine.removeFiles(study, fileNames, outdir);
        // Update study configuration to synchronize
        synchronizeCatalogStudyFromStorage(study, token, true, fileNames);

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
        VariantStorageMetadataManager metadataManager = variantStorageEngine.getMetadataManager();
        CatalogStorageMetadataSynchronizer metadataSynchronizer
                = new CatalogStorageMetadataSynchronizer(catalogManager, metadataManager);

        // Get all files related to the samples
        Set<String> allFilesFromSamples = new HashSet<>();
        for (Sample sample : catalogManager.getSampleManager().get(study, samples,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.FILE_IDS.key()), token).getResults()) {
            if (sample.getFileIds() != null) {
                allFilesFromSamples.addAll(sample.getFileIds());
            }
        }
        // Get only the files that are indexed.
        List<String> fileIds = new ArrayList<>(allFilesFromSamples.size());
        for (File file : catalogManager.getFileManager().search(study,
                new Query(FileDBAdaptor.QueryParams.ID.key(), allFilesFromSamples)
                        .append(FileDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX_STATUS_ID.key(), VariantIndexStatus.READY),
                new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.ID.key()), token).getResults()) {
            fileIds.add(file.getId());
        }

        // Update Catalog file and cohort status.
        metadataSynchronizer.synchronizeCatalogFromStorage(study, token);
        synchronizeCatalogStudyFromStorage(study, token, true, fileIds);
        metadataSynchronizer.synchronizeCatalogSamplesFromStorage(study, samples, token);
    }


}
