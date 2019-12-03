/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.analysis.variant.operations;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.DataStore;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

/**
 * Created by pfurio on 23/08/16.
 */
public abstract class StorageOperation extends OpenCgaAnalysis {

    public static final String KEEP_INTERMEDIATE_FILES = "keepIntermediateFiles";

    protected Boolean keepIntermediateFiles;

    @Override
    protected void check() throws Exception {
        super.check();
        if (keepIntermediateFiles == null) {
            keepIntermediateFiles = params.getBoolean(KEEP_INTERMEDIATE_FILES);
        }
    }

    public StorageOperation setKeepIntermediateFiles(Boolean keepIntermediateFiles) {
        this.keepIntermediateFiles = keepIntermediateFiles;
        return this;
    }

    public final StudyMetadata synchronizeCatalogStudyFromStorage(DataStore dataStore, String study, String sessionId)
            throws CatalogException, StorageEngineException {
        VariantStorageMetadataManager metadataManager = getVariantStorageEngine(dataStore).getMetadataManager();
        CatalogStorageMetadataSynchronizer metadataSynchronizer
                = new CatalogStorageMetadataSynchronizer(catalogManager, metadataManager);

        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        if (studyMetadata != null) {
            // Update Catalog file and cohort status.
            metadataSynchronizer.synchronizeCatalogStudyFromStorage(studyMetadata, sessionId);
        }
        return studyMetadata;
    }

    protected final VariantStorageEngine getVariantStorageEngine(String study) throws CatalogException, StorageEngineException {
        return getVariantStorageEngine(variantStorageManager.getDataStore(study, token));
    }

    protected final VariantStorageEngine getVariantStorageEngineByProject(String project) throws CatalogException, StorageEngineException {
        return getVariantStorageEngine(variantStorageManager.getDataStoreByProjectId(project, token));
    }

    protected final VariantStorageEngine getVariantStorageEngine(DataStore dataStore) throws StorageEngineException {
        return StorageEngineFactory.get(variantStorageManager.getStorageConfiguration())
                .getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
    }

    public static boolean isVcfFormat(File file) {
        File.Format format = file.getFormat();
        if (isVcfFormat(format)) {
            return true;
        } else {
            // Do not trust the file format. Defect format from URI
            format = org.opencb.opencga.catalog.managers.FileUtils.detectFormat(file.getUri());
            if (isVcfFormat(format)) {
                // Overwrite temporary the format
                file.setFormat(format);
                return true;
            } else {
                return false;
            }
        }
    }

    private static boolean isVcfFormat(File.Format format) {
        return format.equals(File.Format.VCF) || format.equals(File.Format.GVCF) || format.equals(File.Format.BCF);
    }

}
