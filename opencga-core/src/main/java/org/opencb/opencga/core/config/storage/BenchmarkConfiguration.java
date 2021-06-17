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

package org.opencb.opencga.core.config.storage;

import java.net.URI;
import java.util.List;

/**
 * Created by imedina on 08/10/15.
 */
public class BenchmarkConfiguration {

    private int numRepetitions;
    private boolean load;
    private List<String> queries;

    private String databaseName;

    private String mode;
    private int delay;
    private String connectionType;
    private int concurrency;
    private URI rest;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BenchmarkConfiguration{");
        sb.append(", numRepetitions=").append(numRepetitions);
        sb.append(", load=").append(load);
        sb.append(", queries=").append(queries);
        sb.append(", databaseName='").append(databaseName).append('\'');
        sb.append(", mode='").append(mode).append('\'');
        sb.append(", delay=").append(delay);
        sb.append(", connectionType='").append(connectionType).append('\'');
        sb.append(", concurrency=").append(concurrency);
        sb.append(", rest=").append(rest);
        sb.append('}');
        return sb.toString();
    }

    public String getConnectionType() {
        return connectionType;
    }

    public BenchmarkConfiguration setConnectionType(String connectionType) {
        this.connectionType = connectionType;
        return this;
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

    public String getMode() {
        return mode;
    }

    public BenchmarkConfiguration setMode(String mode) {
        this.mode = mode;
        return this;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public URI getRest() {
        return rest;
    }

    public BenchmarkConfiguration setRest(URI rest) {
        this.rest = rest;
        return this;
    }

    public int getDelay() {
        return delay;
    }

    public BenchmarkConfiguration setDelay(int delay) {
        this.delay = delay;
        return this;
    }
}
