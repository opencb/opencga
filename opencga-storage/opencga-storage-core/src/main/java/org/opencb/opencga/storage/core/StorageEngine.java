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

package org.opencb.opencga.storage.core;

import org.opencb.opencga.core.common.MemoryUsageMonitor;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.io.managers.IOManagerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * This class manages all the operations with storage and provides a DBAdaptor.
 */
public abstract class StorageEngine<DBADAPTOR> implements AutoCloseable {

    protected String storageEngineId;
    protected StorageConfiguration configuration;
    protected String dbName;
    protected IOManagerProvider ioManagerProvider;

    private Logger logger = LoggerFactory.getLogger(StorageEngine.class);

    public StorageEngine() {
    }

    public StorageEngine(StorageConfiguration configuration) {
        this(configuration.getDefaultStorageEngineId(), configuration);
//        this.configuration = configuration;
    }

    public StorageEngine(String storageEngineId, StorageConfiguration configuration) {
        setConfiguration(configuration, storageEngineId);
    }

    @Deprecated
    public void setConfiguration(StorageConfiguration configuration, String storageEngineId) {
        setConfiguration(configuration, storageEngineId, "");
    }

    public void setConfiguration(StorageConfiguration configuration, String storageEngineId, String dbName) {
        this.configuration = configuration;
        this.storageEngineId = storageEngineId;
        this.dbName = dbName;
        this.ioManagerProvider = createIOManagerProvider(configuration);
    }

    protected IOManagerProvider createIOManagerProvider(StorageConfiguration configuration) {
        return new IOManagerProvider(configuration);
    }

    public String getStorageEngineId() {
        return storageEngineId;
    }

    public String getDBName() {
        return dbName;
    }

    public List<StoragePipelineResult> index(List<URI> inputFiles, URI outdirUri, boolean doExtract, boolean doTransform, boolean doLoad)
            throws StorageEngineException {

        List<StoragePipelineResult> results = new ArrayList<>(inputFiles.size());

        // Check the database connection before we start
        if (doLoad) {
            testConnection();
        }

        for (URI inputFile : inputFiles) {
            //Provide a connected storagePipeline if load is required.
            StoragePipeline storagePipeline = newStoragePipeline(doLoad);

            StoragePipelineResult result = new StoragePipelineResult(inputFile);
            results.add(result);

            URI nextFileUri = inputFile;
            result.setInput(inputFile);

            if (doExtract) {
                logger.info("Extract '{}'", inputFile);
                nextFileUri = storagePipeline.extract(inputFile, outdirUri);
                result.setExtractResult(nextFileUri);
            }

            if (doTransform) {
                nextFileUri = transformFile(storagePipeline, result, results, nextFileUri, outdirUri);
            }

            if (doLoad) {
                loadFile(storagePipeline, result, results, nextFileUri, outdirUri);
            }

            storagePipeline.close();

            MemoryUsageMonitor.logMemory(logger);
        }

        return results;
    }

    protected void loadFile(StoragePipeline storagePipeline, StoragePipelineResult result, List<StoragePipelineResult> results,
                            URI inputFileUri, URI outdirUri) throws StoragePipelineException {
        result.setLoadExecuted(true);
        long millis = System.currentTimeMillis();
        try {
            logger.info("PreLoad '{}'", inputFileUri);
            inputFileUri = storagePipeline.preLoad(inputFileUri, outdirUri);
            result.setPreLoadResult(inputFileUri);

            logger.info("Load '{}'", inputFileUri);
            inputFileUri = storagePipeline.load(inputFileUri);
            result.setLoadResult(inputFileUri);

            logger.info("PostLoad '{}'", inputFileUri);
            inputFileUri = storagePipeline.postLoad(inputFileUri, outdirUri);
            result.setPostLoadResult(inputFileUri);
        } catch (Exception e) {
            result.setLoadError(e);
            throw new StoragePipelineException("Exception executing load: " + e.getMessage(), e, results);
        } finally {
            result.setLoadTimeMillis(System.currentTimeMillis() - millis);
            result.setLoadStats(storagePipeline.getLoadStats());
        }
    }

    protected URI transformFile(StoragePipeline storagePipeline, StoragePipelineResult result, List<StoragePipelineResult> results,
                                URI inputFileUri, URI outdirUri) throws StoragePipelineException {
        result.setTransformExecuted(true);
        long millis = System.currentTimeMillis();
        try {
            logger.info("PreTransform '{}'", inputFileUri);
            inputFileUri = storagePipeline.preTransform(inputFileUri);
            result.setPreTransformResult(inputFileUri);

            logger.info("Transform '{}'", inputFileUri);
            inputFileUri = storagePipeline.transform(inputFileUri, null, outdirUri);
            result.setTransformResult(inputFileUri);

            logger.info("PostTransform '{}'", inputFileUri);
            inputFileUri = storagePipeline.postTransform(inputFileUri);
            result.setPostTransformResult(inputFileUri);
        } catch (Exception e) {
            result.setTransformError(e);
            throw new StoragePipelineException("Exception executing transform.", e, results);
        } finally {
            result.setTransformTimeMillis(System.currentTimeMillis() - millis);
            result.setTransformStats(storagePipeline.getTransformStats());
        }
        return inputFileUri;
    }

    public abstract DBADAPTOR getDBAdaptor() throws StorageEngineException;

    public abstract void testConnection() throws StorageEngineException;

    /**
     * Creates a new {@link StoragePipeline} object.
     *
     * Each {@link StoragePipeline} should be used to index one single file.
     *
     * @param connected Specify if the provided object must be connected to the underlying database.
     * @return Created {@link StoragePipeline}
     * @throws StorageEngineException If there is any problem while creation
     */
    public abstract StoragePipeline newStoragePipeline(boolean connected) throws StorageEngineException;


    public StorageConfiguration getConfiguration() {
        return configuration;
    }

    public IOManagerProvider getIOManagerProvider() {
        return ioManagerProvider;
    }

    @Override
    public void close() throws Exception {
    }
}
