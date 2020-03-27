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

package org.opencb.opencga.client.config;

/**
 * Created by imedina on 04/05/16.
 */
public class RestConfig {

    private String host;
    private boolean tokenAutoRefresh;
    private QueryRestConfig query;

    public RestConfig() {
    }

    public RestConfig(String host, boolean tokenAutoRefresh, QueryRestConfig query) {
        this.host = host;
        this.tokenAutoRefresh = tokenAutoRefresh;
        this.query = query;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RestConfig{");
        sb.append("host='").append(host).append('\'');
        sb.append(", tokenAutoRefresh=").append(tokenAutoRefresh);
        sb.append(", query=").append(query);
        sb.append('}');
        return sb.toString();
    }

    public String getHost() {
        return host;
    }

    public RestConfig setHost(String host) {
        this.host = host;
        return this;
    }

    public boolean isTokenAutoRefresh() {
        return tokenAutoRefresh;
    }

    public RestConfig setTokenAutoRefresh(boolean tokenAutoRefresh) {
        this.tokenAutoRefresh = tokenAutoRefresh;
        return this;
    }

    public QueryRestConfig getQuery() {
        return query;
    }

    public RestConfig setQuery(QueryRestConfig query) {
        this.query = query;
        return this;
    }
}
