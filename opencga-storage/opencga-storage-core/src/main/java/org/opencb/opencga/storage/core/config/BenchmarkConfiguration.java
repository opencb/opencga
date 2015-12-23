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

    private List<String> storageEngines;
    private int numRepetitions;
    private boolean load;

    private List<String> queries;
    private List<String> tables;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BenchmarkConfiguration{");
        sb.append("storageEngines=").append(storageEngines);
        sb.append(", numRepetitions=").append(numRepetitions);
        sb.append(", load=").append(load);
        sb.append(", queries=").append(queries);
        sb.append(", tables=").append(tables);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getStorageEngines() {
        return storageEngines;
    }

    public void setStorageEngines(List<String> storageEngines) {
        this.storageEngines = storageEngines;
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

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }
}
