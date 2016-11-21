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

package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageTest;
import org.opencb.opencga.storage.mongodb.auth.MongoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by hpccoll1 on 01/06/15.
 */
public interface MongoDBVariantStorageTest extends VariantStorageTest {

    Logger logger = LoggerFactory.getLogger(MongoDBVariantStorageTest.class);
    AtomicReference<MongoDBVariantStorageManager> manager = new AtomicReference<>(null);

    default MongoDBVariantStorageManager getVariantStorageManager() throws Exception {
        synchronized (manager) {
            MongoDBVariantStorageManager storageManager = manager.get();
            if (storageManager == null) {
                storageManager = new MongoDBVariantStorageManager();
                manager.set(storageManager);
            }
            InputStream is = MongoDBVariantStorageTest.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
            StorageConfiguration storageConfiguration = StorageConfiguration.load(is);
            storageManager.setConfiguration(storageConfiguration, MongoDBVariantStorageManager.STORAGE_ENGINE_ID);
            return storageManager;
        }
    }

    default MongoDBVariantStorageManager newVariantStorageManager() throws Exception {
        synchronized (manager) {
            MongoDBVariantStorageManager storageManager = new MongoDBVariantStorageManager();
            InputStream is = MongoDBVariantStorageTest.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
            StorageConfiguration storageConfiguration = StorageConfiguration.load(is);
            storageManager.setConfiguration(storageConfiguration, MongoDBVariantStorageManager.STORAGE_ENGINE_ID);
            return storageManager;
        }
    }

    default void clearDB(String dbName) throws Exception {
        MongoCredentials credentials = getVariantStorageManager().getMongoCredentials(dbName);
        logger.info("Cleaning MongoDB {}", credentials.getMongoDbName());
        try (MongoDataStoreManager mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses())) {
            MongoDataStore mongoDataStore = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
            mongoManager.drop(credentials.getMongoDbName());
        }
    }

    default int getExpectedNumLoadedVariants(VariantSource source) {
        int numRecords = source.getStats().getNumRecords();
        return numRecords
                - source.getStats().getVariantTypeCount(VariantType.SYMBOLIC)
                - source.getStats().getVariantTypeCount(VariantType.NO_VARIATION);
    }


    default MongoDataStoreManager getMongoDataStoreManager(String dbName) throws Exception {
        MongoCredentials credentials = getVariantStorageManager().getMongoCredentials(dbName);
        return new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
    }
}
