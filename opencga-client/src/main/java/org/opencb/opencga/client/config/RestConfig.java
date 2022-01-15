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

package org.opencb.opencga.client.config;

import java.util.List;

/**
 * Created by imedina on 04/05/16.
 */
public class RestConfig {

    private List<HostConfig> hosts;
    private int defaultHostIndex = 0;
    private boolean tokenAutoRefresh;
    private boolean tlsAllowInvalidCertificates;
    private QueryRestConfig query;

    public RestConfig() {
    }

    public RestConfig(List<HostConfig> hosts, boolean tokenAutoRefresh, QueryRestConfig query) {
        this(hosts, 0, tokenAutoRefresh, false, query);
    }

    public RestConfig(List<HostConfig> hosts, boolean tokenAutoRefresh, boolean tlsAllowInvalidCertificates,
                      QueryRestConfig query) {
        this(hosts, 0, tokenAutoRefresh, tlsAllowInvalidCertificates, query);
    }

    public RestConfig(List<HostConfig> hosts, int defaultHostIndex, boolean tokenAutoRefresh,
                      boolean tlsAllowInvalidCertificates, QueryRestConfig query) {
        this.hosts = hosts;
        this.defaultHostIndex = hosts != null && hosts.size() > 0 ? defaultHostIndex : -1;
        this.tokenAutoRefresh = tokenAutoRefresh;
        this.tlsAllowInvalidCertificates = tlsAllowInvalidCertificates;
        this.query = query;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RestConfig{");
        sb.append("hosts=").append(hosts);
        sb.append(", tokenAutoRefresh=").append(tokenAutoRefresh);
        sb.append(", tlsAllowInvalidCertificates=").append(tlsAllowInvalidCertificates);
        sb.append(", query=").append(query);
        sb.append('}');
        return sb.toString();
    }

    public List<HostConfig> getHosts() {
        return hosts;
    }

    public RestConfig setHosts(List<HostConfig> hosts) {
        this.hosts = hosts;
        return this;
    }

    public boolean isTokenAutoRefresh() {
        return tokenAutoRefresh;
    }

    public RestConfig setTokenAutoRefresh(boolean tokenAutoRefresh) {
        this.tokenAutoRefresh = tokenAutoRefresh;
        return this;
    }

    public boolean isTlsAllowInvalidCertificates() {
        return tlsAllowInvalidCertificates;
    }

    public RestConfig setTlsAllowInvalidCertificates(boolean tlsAllowInvalidCertificates) {
        this.tlsAllowInvalidCertificates = tlsAllowInvalidCertificates;
        return this;
    }

    public QueryRestConfig getQuery() {
        return query;
    }

    public RestConfig setQuery(QueryRestConfig query) {
        this.query = query;
        return this;
    }

    public int getDefaultHostIndex() {
        return defaultHostIndex;
    }

    public RestConfig setDefaultHostIndex(int defaultHostIndex) {
        this.defaultHostIndex = defaultHostIndex;
        return this;
    }
}
