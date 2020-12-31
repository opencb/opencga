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

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by pfurio on 23/08/16.
 */
public abstract class OperationManager {

    protected final CatalogManager catalogManager;
    protected final VariantStorageManager variantStorageManager;
    protected final VariantStorageEngine variantStorageEngine;

    public OperationManager(VariantStorageManager variantStorageManager, VariantStorageEngine variantStorageEngine) {
        this.variantStorageManager = variantStorageManager;
        this.catalogManager = variantStorageManager.getCatalogManager();
        this.variantStorageEngine = variantStorageEngine;
    }

    public final StudyMetadata synchronizeCatalogStudyFromStorage(String study, String sessionId)
            throws CatalogException, StorageEngineException {
        VariantStorageMetadataManager metadataManager = variantStorageEngine.getMetadataManager();
        CatalogStorageMetadataSynchronizer metadataSynchronizer
                = new CatalogStorageMetadataSynchronizer(catalogManager, metadataManager);

        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        if (studyMetadata != null) {
            // Update Catalog file and cohort status.
            metadataSynchronizer.synchronizeCatalogStudyFromStorage(studyMetadata, sessionId);
        }
        return studyMetadata;
    }

    protected final String getStudyFqn(String study, String token) throws CatalogException {
        return catalogManager.getStudyManager().get(study, StudyManager.INCLUDE_STUDY_ID, token).first().getFqn();
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

    protected void toFileSystemPath(String studyFqn, ObjectMap params, String key, String token) throws CatalogException {
        String file = params.getString(key);
        if (StringUtils.isNotEmpty(file)) {
            Path filePath = getFileSystemPath(studyFqn, file, token);
            params.put(key, filePath.toString());
        }
    }

    protected Path getFileSystemPath(String studyFqn, String fileId, String token) throws CatalogException {
        File file = catalogManager.getFileManager().get(studyFqn, fileId,
                new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.URI.key()), token).first();
        return Paths.get(file.getUri()).toAbsolutePath();
    }
}
