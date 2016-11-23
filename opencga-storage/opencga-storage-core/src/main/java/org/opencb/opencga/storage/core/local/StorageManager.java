/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.core.local;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.storage.core.cache.CacheManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.slf4j.Logger;

import javax.annotation.Nullable;


public abstract class StorageManager {

    protected CatalogManager catalogManager;
    protected CacheManager cacheManager;
    protected StorageConfiguration storageConfiguration;

    @Deprecated
    protected String storageEngineId;

    protected Logger logger;


    public StorageManager() {
    }

    public StorageManager(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        this.init(catalogManager, new CacheManager(storageConfiguration), storageConfiguration,
                storageConfiguration.getDefaultStorageEngineId());
    }

    public StorageManager(CatalogManager catalogManager, CacheManager cacheManager, StorageConfiguration storageConfiguration) {
        this.init(catalogManager, cacheManager, storageConfiguration, storageConfiguration.getDefaultStorageEngineId());
    }

    @Deprecated
    public StorageManager(CatalogManager catalogManager, StorageConfiguration storageConfiguration, String storageEngineId) {
        this.init(catalogManager, new CacheManager(storageConfiguration), storageConfiguration, storageEngineId);
    }


    protected void init(CatalogManager catalogManager, CacheManager cacheManager, StorageConfiguration storageConfiguration,
                        String storageEngineId) {
        this.catalogManager = catalogManager;
        this.cacheManager = cacheManager;
        this.storageConfiguration = storageConfiguration;
        this.storageEngineId = storageEngineId;
    }


    public void clearCache(String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);

    }


    public void clearCache(String studyId, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);

    }


    public abstract void testConnection() throws StorageManagerException;

    /**
     * Given the file and study string, retrieve the corresponding long ids.
     *
     * @param studyIdStr study string.
     * @param fileIdStr file string.
     * @param sessionId session id.
     * @return an objectMap containing the keys "fileId" and "studyId"
     * @throws CatalogException catalog exception.
     */
    protected ObjectMap getFileAndStudyId(@Nullable String studyIdStr, String fileIdStr, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = 0;
        if (StringUtils.isNotEmpty(studyIdStr)) {
            studyId = catalogManager.getStudyManager().getId(userId, studyIdStr);
        }

        long fileId;
        if (studyId > 0) {
            fileId = catalogManager.getFileManager().getId(userId, studyId, fileIdStr);
            if (fileId <= 0) {
                throw new CatalogException("The id of file " + fileIdStr + " could not be found under study " + studyIdStr);
            }
        } else {
            fileId = catalogManager.getFileManager().getId(userId, fileIdStr);
            if (fileId <= 0) {
                throw new CatalogException("The id of file " + fileIdStr + " could not be found");
            }
            studyId = catalogManager.getFileManager().getStudyId(fileId);
        }

        return new ObjectMap("fileId", fileId).append("studyId", studyId);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StorageManager{");
        sb.append("catalogManager=").append(catalogManager);
        sb.append(", cacheManager=").append(cacheManager);
        sb.append(", storageConfiguration=").append(storageConfiguration);
        sb.append(", storageEngineId='").append(storageEngineId).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    public StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }
}
