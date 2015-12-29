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

import org.opencb.datastore.core.ObjectMap;

/**
 * Created by imedina on 09/05/15.
 */
public class StorageEtlConfiguration {

    private String manager;

    /**
     * options parameter defines database-specific parameters.
     */
    private ObjectMap options;
    private DatabaseCredentials database;


    public StorageEtlConfiguration() {
    }

//    public StorageEtlConfiguration(String manager, Map<String, String> options, DatabaseCredentials database) {
//        this.manager = manager;
//        this.options = options;
//        this.database = database;
//    }

    public StorageEtlConfiguration(String manager, ObjectMap options, DatabaseCredentials database) {
        this.manager = manager;
        this.options = options;
        this.database = database;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StorageEtlConfiguration{");
        sb.append("manager='").append(manager).append('\'');
        sb.append(", options=").append(options);
        sb.append(", database=").append(database);
        sb.append('}');
        return sb.toString();
    }

    public String getManager() {
        return manager;
    }

    public void setManager(String manager) {
        this.manager = manager;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public void setOptions(ObjectMap options) {
        this.options = options;
    }

    public DatabaseCredentials getDatabase() {
        return database;
    }

    public void setDatabase(DatabaseCredentials database) {
        this.database = database;
    }

}
