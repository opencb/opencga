/*
 * Copyright 2015 OpenCB
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

import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.slf4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @param <DBADAPTOR>
 * @author imedina
 */
public abstract class StorageManager<DBADAPTOR> {

    protected String storageEngineId;
    protected StorageConfiguration configuration;
    protected Logger logger;

    public StorageManager() {
    }

    public StorageManager(StorageConfiguration configuration) {
        this(configuration.getDefaultStorageEngineId(), configuration);
//        this.configuration = configuration;
    }

    public StorageManager(String storageEngineId, StorageConfiguration configuration) {
        this.storageEngineId = storageEngineId;
        this.configuration = configuration;
    }

    public void setConfiguration(StorageConfiguration configuration, String storageEngineId) {
        this.configuration = configuration;
        this.storageEngineId = storageEngineId;
    }

    public StorageConfiguration getConfiguration() {
        return configuration;
    }

    public String getStorageEngineId() {
        return storageEngineId;
    }

    public List<StorageETLResult> index(List<URI> inputFiles, URI outdirUri, boolean doExtract, boolean doTransform, boolean doLoad)
            throws StorageManagerException {

        List<StorageETLResult> results = new ArrayList<>(inputFiles.size());
        boolean abortOnFail = true;

        // Check the database connection before we start
        if (doLoad) {
            testConnection();
        }

        for (URI inputFile : inputFiles) {
            //Provide a connected storageETL if load is required.
            StorageETL storageETL = newStorageETL(doLoad);

            StorageETLResult etlResult = new StorageETLResult();
            results.add(etlResult);

            URI nextFileUri = inputFile;
            etlResult.setInput(inputFile);

            if (doExtract) {
                logger.info("Extract '{}'", inputFile);
                nextFileUri = storageETL.extract(inputFile, outdirUri);
                etlResult.setExtractResult(nextFileUri);
            }

            if (doTransform) {
                etlResult.setTransformExecuted(true);
                long millis = System.currentTimeMillis();
                try {
                    logger.info("PreTransform '{}'", nextFileUri);
                    nextFileUri = storageETL.preTransform(nextFileUri);
                    etlResult.setPreTransformResult(nextFileUri);

                    logger.info("Transform '{}'", nextFileUri);
                    nextFileUri = storageETL.transform(nextFileUri, null, outdirUri);
                    etlResult.setTransformResult(nextFileUri);

                    logger.info("PostTransform '{}'", nextFileUri);
                    nextFileUri = storageETL.postTransform(nextFileUri);
                    etlResult.setPostTransformResult(nextFileUri);
                } catch (Exception e) {
                    etlResult.setTransformError(e);
                    if (abortOnFail) {
                        throw new StorageETLException("Exception executing transform.", e, results);
                    } else {
                        continue;
                    }
                } finally {
                    etlResult.setTransformTimeMillis(System.currentTimeMillis() - millis);
                    etlResult.setTransformStats(storageETL.getTransformStats());
                }
            }

            if (doLoad) {
                etlResult.setLoadExecuted(true);
                long millis = System.currentTimeMillis();
                try {
                    logger.info("PreLoad '{}'", nextFileUri);
                    nextFileUri = storageETL.preLoad(nextFileUri, outdirUri);
                    etlResult.setPreLoadResult(nextFileUri);

                    logger.info("Load '{}'", nextFileUri);
                    nextFileUri = storageETL.load(nextFileUri);
                    etlResult.setLoadResult(nextFileUri);

                    logger.info("PostLoad '{}'", nextFileUri);
                    nextFileUri = storageETL.postLoad(nextFileUri, outdirUri);
                    etlResult.setPostLoadResult(nextFileUri);
                } catch (Exception e) {
                    etlResult.setLoadError(e);
                    if (abortOnFail) {
                        throw new StorageETLException("Exception executing load.", e, results);
                    }
                } finally {
                    etlResult.setLoadTimeMillis(System.currentTimeMillis() - millis);
                    etlResult.setLoadStats(storageETL.getLoadStats());
                }
            }
        }

        return results;
    }

    public abstract DBADAPTOR getDBAdaptor(String dbName) throws StorageManagerException;

    // TODO: Pending implementation
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

}
