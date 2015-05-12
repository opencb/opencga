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

package org.opencb.opencga.storage.core.config;

import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 01/05/15.
 */
public class StorageConfigurationTest {

    @Test
    public void testDefault() {
        StorageConfiguration storageConfiguration = new StorageConfiguration();

        Map<String, String> options = new HashMap<>();
        options.put("key", "value");

        StorageEngineConfiguration storageEngineConfiguration1 = new StorageEngineConfiguration(
                "mongodb",
                new StorageEtlConfiguration("org.opencb.opencga.storage.mongodb.alignment.MongoDBAlignmentStorageManager", Collections.EMPTY_MAP, new DatabaseCredentials(Arrays.asList("mongodb-dev:27017"), "user", "password", options)),
                new StorageEtlConfiguration("org.opencb.opencga.storage.mongodb.alignment.MongoDBVariantStorageManager", Collections.EMPTY_MAP, new DatabaseCredentials(Arrays.asList("mongodb-dev:27017"), "user", "password", options)),
                new HashMap<>());

        StorageEngineConfiguration storageEngineConfiguration2 = new StorageEngineConfiguration(
                "hbase",
                new StorageEtlConfiguration("org.opencb.opencga.storage.hbase.alignment.MongoDBAlignmentStorageManager", Collections.EMPTY_MAP, new DatabaseCredentials(Arrays.asList("who-master:60000"), "user", "password", Collections.emptyMap())),
                new StorageEtlConfiguration("org.opencb.opencga.storage.hbase.alignment.MongoDBVariantStorageManager", Collections.EMPTY_MAP, new DatabaseCredentials(Arrays.asList("who-master:60000"), "user", "password", Collections.emptyMap())),
                new HashMap<>());

//        StorageEngineConfiguration storageEngineConfiguration2 = new StorageEngineConfiguration(
//                "hbase",
//                "org.opencb.opencga.storage.mongodb.alignment.MongoDBAlignmentStorageManager",
//                "org.opencb.opencga.storage.mongodb.alignment.MongoDBVariantStorageManager",
//                new DatabaseCredentials(Arrays.asList("who-master:60000"), "user", "password", Collections.emptyMap()),
//                new DatabaseCredentials(Arrays.asList("who-master:60000"), "user", "password", Collections.emptyMap()),
//                null,
//                new HashMap<>());

//        StorageEngineConfiguration storageEngineConfiguration2 = new StorageEngineConfiguration(
//                "hbase", new DatabaseCredentials(Arrays.asList("who-master:60000"), "user", "password", Collections.emptyMap()));

        storageConfiguration.setDefaultStorageEngine("mongodb");
        storageConfiguration.getEngines().add(storageEngineConfiguration1);
        storageConfiguration.getEngines().add(storageEngineConfiguration2);

        try {
            storageConfiguration.serialize(new FileOutputStream("/tmp/aaa"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLoad() throws Exception {

    }


}