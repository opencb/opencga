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

package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;


/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class MongoVariantStorageManagerTest extends VariantStorageManagerTest {

    static protected MongoDBVariantStorageManager manager = null;
    @Override
    protected MongoDBVariantStorageManager getVariantStorageManager() throws Exception {
        if (manager == null) {
            manager = new MongoDBVariantStorageManager();
            manager.addConfigUri(this.getClass().getClassLoader().getResource("storage-mongo-test.properties").toURI());
        }
        return manager;
    }

    @Override
    protected void clearDB() throws Exception {
        MongoCredentials credentials = getVariantStorageManager().getMongoCredentials();
        logger.info("Cleaning MongoDB {}" , credentials.getMongoDbName());
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        MongoDataStore mongoDataStore = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        mongoManager.drop(credentials.getMongoDbName());
    }

}
