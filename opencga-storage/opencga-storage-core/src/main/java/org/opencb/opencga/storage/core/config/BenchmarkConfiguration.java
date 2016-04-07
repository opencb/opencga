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

import java.util.List;

/**
 * Created by imedina on 08/10/15.
 */
public class BenchmarkConfiguration {

    private String storageEngine;
    private int numRepetitions;
    private boolean load;
    private List<String> queries;

    private String databaseName;
    private String table;
    private DatabaseCredentials database;
    private int concurrency;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BenchmarkConfiguration{");
        sb.append("storageEngine='").append(storageEngine).append('\'');
        sb.append(", numRepetitions=").append(numRepetitions);
        sb.append(", load=").append(load);
        sb.append(", queries=").append(queries);
        sb.append(", databaseName='").append(databaseName).append('\'');
        sb.append(", table='").append(table).append('\'');
        sb.append(", database=").append(database);
        sb.append(", concurrency=").append(concurrency);
        sb.append('}');
        return sb.toString();
    }

    public String getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(String storageEngine) {
        this.storageEngine = storageEngine;
    }

    public int getNumRepetitions() {
        return numRepetitions;
    }

    public void setNumRepetitions(int numRepetitions) {
        this.numRepetitions = numRepetitions;
    }

    public boolean isLoad() {
        return load;
    }

    public void setLoad(boolean load) {
        this.load = load;
    }

    public List<String> getQueries() {
        return queries;
    }

    public void setQueries(List<String> queries) {
        this.queries = queries;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public DatabaseCredentials getDatabase() {
        return database;
    }

    public void setDatabase(DatabaseCredentials database) {
        this.database = database;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }
}
