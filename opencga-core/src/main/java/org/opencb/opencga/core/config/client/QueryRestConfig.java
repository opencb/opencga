/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.core.config.client;

public class QueryRestConfig {

    private int batchSize;
    private int limit;

    public QueryRestConfig() {
    }

    public QueryRestConfig(int batchSize, int limit) {
        this.batchSize = batchSize;
        this.limit = limit;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("QueryRestConfig{");
        sb.append("batchSize=").append(batchSize);
        sb.append(", limit=").append(limit);
        sb.append('}');
        return sb.toString();
    }

    public int getBatchSize() {
        return batchSize;
    }

    public QueryRestConfig setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public int getLimit() {
        return limit;
    }

    public QueryRestConfig setLimit(int limit) {
        this.limit = limit;
        return this;
    }
}
