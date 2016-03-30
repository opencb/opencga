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

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
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

    public List<ObjectMap> index(List<URI> inputFiles, URI outdirUri, boolean extract, boolean transform, boolean load)
            throws StorageManagerException, IOException, FileFormatException {

        for (URI inputFile : inputFiles) {
            StorageETL storageETL = newStorageETL();

            URI nextFileUri = inputFile;

            // Check the database connection before we start
            if (load) {
                testConnection();
            }

            if (extract) {
                logger.info("Extract '{}'", inputFile);
                nextFileUri = storageETL.extract(inputFile, outdirUri);
            }

            if (transform) {
                logger.info("PreTransform '{}'", nextFileUri);
                nextFileUri = storageETL.preTransform(nextFileUri);
                logger.info("Transform '{}'", nextFileUri);
                nextFileUri = storageETL.transform(nextFileUri, null, outdirUri);
                logger.info("PostTransform '{}'", nextFileUri);
                nextFileUri = storageETL.postTransform(nextFileUri);
            }

            if (load) {
                logger.info("PreLoad '{}'", nextFileUri);
                nextFileUri = storageETL.preLoad(nextFileUri, outdirUri);
                logger.info("Load '{}'", nextFileUri);
                nextFileUri = storageETL.load(nextFileUri);
                logger.info("PostLoad '{}'", nextFileUri);
                nextFileUri = storageETL.postLoad(nextFileUri, outdirUri);
            }
        }

        return Collections.emptyList();
    }

    public abstract DBADAPTOR getDBAdaptor(String dbName) throws StorageManagerException;

    // TODO: Pending implementation
    public abstract void testConnection() throws StorageManagerException;

    protected abstract StorageETL newStorageETL();

}
