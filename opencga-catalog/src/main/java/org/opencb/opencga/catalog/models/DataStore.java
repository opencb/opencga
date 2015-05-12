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

package org.opencb.opencga.catalog.models;

/**
 * Created by jacobo on 14/04/15.
 *
 *
 */
public class DataStore {
    private String storageEngine;
    private String dbName;

    public DataStore() {
    }

    public DataStore(String storageEngine, String dbName) {
        this.storageEngine = storageEngine;
        this.dbName = dbName;
    }

    @Override
    public String toString() {
        return "DataStore{" +
                "storageEngine='" + storageEngine + '\'' +
                ", dbName='" + dbName + '\'' +
                '}';
    }

    public String getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(String storageEngine) {
        this.storageEngine = storageEngine;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
