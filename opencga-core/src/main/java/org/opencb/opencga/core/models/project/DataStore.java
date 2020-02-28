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

package org.opencb.opencga.core.models.project;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Objects;

/**
 * Created by jacobo on 14/04/15.
 */
public class DataStore {

    private String storageEngine;
    private String dbName;
    private ObjectMap configuration;

    public DataStore() {
    }

    public DataStore(String storageEngine, String dbName) {
        this.storageEngine = storageEngine;
        this.dbName = dbName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DataStore{");
        sb.append("storageEngine='").append(storageEngine).append('\'');
        sb.append(", dbName='").append(dbName).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getStorageEngine() {
        return storageEngine;
    }

    public DataStore setStorageEngine(String storageEngine) {
        this.storageEngine = storageEngine;
        return this;
    }

    public String getDbName() {
        return dbName;
    }

    public DataStore setDbName(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public ObjectMap getConfiguration() {
        return configuration;
    }

    public DataStore setConfiguration(ObjectMap configuration) {
        this.configuration = configuration;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataStore)) {
            return false;
        }
        DataStore dataStore = (DataStore) o;
        return Objects.equals(storageEngine, dataStore.storageEngine)
                && Objects.equals(dbName, dataStore.dbName)
                && Objects.equals(configuration, dataStore.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageEngine, dbName, configuration);
    }
}
