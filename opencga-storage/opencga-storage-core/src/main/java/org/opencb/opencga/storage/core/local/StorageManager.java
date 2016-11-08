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

import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.MemoryUsageMonitor;
import org.opencb.opencga.storage.core.StorageETL;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.cache.CacheManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @author imedina
 */
public abstract class StorageManager {

    protected CatalogManager catalogManager;
    protected StorageConfiguration configuration;
    protected String storageEngineId;

    protected CacheManager cacheManager;
    protected Logger logger;

    public StorageManager() {
    }

    public StorageManager(CatalogManager catalogManager, StorageConfiguration configuration) {
        this(catalogManager, configuration, configuration.getDefaultStorageEngineId());
    }

    public StorageManager(CatalogManager catalogManager, StorageConfiguration configuration, String storageEngineId) {
        init(catalogManager, configuration, storageEngineId);
    }

    private void init(CatalogManager catalogManager, StorageConfiguration configuration, String storageEngineId) {
        this.catalogManager = catalogManager;
        this.configuration = configuration;
        this.storageEngineId = storageEngineId;
        this.cacheManager = new CacheManager(configuration);

        logger = LoggerFactory.getLogger(this.getClass());
    }

    public void clearCache(String studyId, String sessionId) {

    }

    public void clearCache(String studyId, String bioformat, String sessionId) {

    }

    public List<StorageETLResult> index(String studyId, List<URI> inputFiles, URI outdirUri, boolean doExtract, boolean doTransform,
                                        boolean doLoad, String sessionId) throws StorageManagerException {

        List<StorageETLResult> results = new ArrayList<>(inputFiles.size());
        boolean abortOnFail = true;

        // Check the database connection before we start
        if (doLoad) {
            testConnection();
        }

        for (URI inputFile : inputFiles) {
            //Provide a connected storageETL if load is required.
            StorageETL storageETL = newStorageETL(doLoad);

            StorageETLResult etlResult = new StorageETLResult(inputFile);
            results.add(etlResult);

            URI nextFileUri = inputFile;
            etlResult.setInput(inputFile);

            if (doExtract) {
                logger.info("Extract '{}'", inputFile);
                nextFileUri = storageETL.extract(inputFile, outdirUri);
                etlResult.setExtractResult(nextFileUri);
            }

            if (doTransform) {
                nextFileUri = transformFile(storageETL, etlResult, results, nextFileUri, outdirUri);
            }

            if (doLoad) {
                loadFile(storageETL, etlResult, results, nextFileUri, outdirUri);
            }

            storageETL.close();

            MemoryUsageMonitor.logMemory(logger);
        }

        return results;
    }

    protected URI transformFile(StorageETL storageETL, StorageETLResult etlResult, List<StorageETLResult> results,
                                URI inputFileUri, URI outdirUri) throws StorageETLException {
        etlResult.setTransformExecuted(true);
        long startMillis = System.currentTimeMillis();
        try {
            logger.info("PreTransform '{}'", inputFileUri);
            inputFileUri = storageETL.preTransform(inputFileUri);
            etlResult.setPreTransformResult(inputFileUri);

            logger.info("Transform '{}'", inputFileUri);
            inputFileUri = storageETL.transform(inputFileUri, null, outdirUri);
            etlResult.setTransformResult(inputFileUri);

            logger.info("PostTransform '{}'", inputFileUri);
            inputFileUri = storageETL.postTransform(inputFileUri);
            etlResult.setPostTransformResult(inputFileUri);
        } catch (Exception e) {
            etlResult.setTransformError(e);
            throw new StorageETLException("Exception executing transform.", e, results);
        } finally {
            etlResult.setTransformTimeMillis(System.currentTimeMillis() - startMillis);
            etlResult.setTransformStats(storageETL.getTransformStats());
        }
        return inputFileUri;
    }

    protected void loadFile(StorageETL storageETL, StorageETLResult etlResult, List<StorageETLResult> results,
                            URI inputFileUri, URI outdirUri) throws StorageETLException {
        etlResult.setLoadExecuted(true);
        long startMillis = System.currentTimeMillis();
        try {
            logger.info("PreLoad '{}'", inputFileUri);
            inputFileUri = storageETL.preLoad(inputFileUri, outdirUri);
            etlResult.setPreLoadResult(inputFileUri);

            logger.info("Load '{}'", inputFileUri);
            inputFileUri = storageETL.load(inputFileUri);
            etlResult.setLoadResult(inputFileUri);

            logger.info("PostLoad '{}'", inputFileUri);
            inputFileUri = storageETL.postLoad(inputFileUri, outdirUri);
            etlResult.setPostLoadResult(inputFileUri);
        } catch (Exception e) {
            etlResult.setLoadError(e);
            throw new StorageETLException("Exception executing load: " + e.getMessage(), e, results);
        } finally {
            etlResult.setLoadTimeMillis(System.currentTimeMillis() - startMillis);
            etlResult.setLoadStats(storageETL.getLoadStats());
        }
    }

    public abstract void testConnection() throws StorageManagerException;

    /**
     * Creates a new {@link StorageETL} object.
     *
     * Each {@link StorageETL} should be used to index one single file.
     *
     * @param connected Specify if the provided object must be connected to the underlying database.
     * @return Created {@link StorageETL}
     * @throws StorageManagerException If there is any problem while creation
     */
    public abstract StorageETL newStorageETL(boolean connected) throws StorageManagerException;


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StorageManager{");
        sb.append("catalogManager=").append(catalogManager);
        sb.append(", configuration=").append(configuration);
        sb.append(", storageEngineId='").append(storageEngineId).append('\'');
        sb.append(", cacheManager=").append(cacheManager);
        sb.append('}');
        return sb.toString();
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public StorageManager setCatalogManager(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        return this;
    }

    public StorageConfiguration getConfiguration() {
        return configuration;
    }

    public StorageManager setConfiguration(StorageConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }

    public String getStorageEngineId() {
        return storageEngineId;
    }

    public StorageManager setStorageEngineId(String storageEngineId) {
        this.storageEngineId = storageEngineId;
        return this;
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    public StorageManager setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        return this;
    }
}
