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

/**
 * Created by imedina on 01/05/15.
 */
public class StorageEngineConfiguration {

    private String id;
    private DatabaseCredentials database;

    public StorageEngineConfiguration() {

    }

    public StorageEngineConfiguration(String id, DatabaseCredentials database) {
        this.id = id;
        this.database = database;
    }


    public DatabaseCredentials getDatabase() {
        return database;
    }

    public void setDatabase(DatabaseCredentials database) {
        this.database = database;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
