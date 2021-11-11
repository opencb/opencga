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

import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * Created by imedina on 04/05/16.
 */
public class RestConfig {

    private List<Host> hosts;
    private boolean tokenAutoRefresh;
    private boolean tlsAllowInvalidCertificates;
    private int timeout;
    private QueryRestConfig query;
    private String url;
    private String hostname;

    public RestConfig() {

    }

    public RestConfig(String defaultClientURL, boolean tokenAutoRefresh, QueryRestConfig query, List<Host> hosts) {
        this.url = defaultClientURL;
        this.hosts = hosts;
        this.tokenAutoRefresh = tokenAutoRefresh;
        this.query = query;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RestConfig{");
        sb.append("host='").append(hosts).append('\'');
        sb.append(", tokenAutoRefresh=").append(tokenAutoRefresh);
        sb.append(", tlsAllowInvalidCertificates=").append(tlsAllowInvalidCertificates);
        sb.append(", query=").append(query);
        sb.append('}');
        return sb.toString();
    }

    public List<Host> getHosts() {
        return hosts;
    }

    public RestConfig setHosts(List<Host> hosts) {
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

    public int getTimeout() {
        return timeout;
    }

    public RestConfig setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public QueryRestConfig getQuery() {
        return query;
    }

    public RestConfig setQuery(QueryRestConfig query) {
        this.query = query;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public RestConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getHostname() {
        return hostname;
    }

    public RestConfig setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    private boolean isDefaultHostAdded() {
        boolean enc = false;
        if (!CollectionUtils.isEmpty(hosts)) {
            for (Host h : hosts) {
                if (hostname.equals(h.getName())) {
                    enc = true;
                    break;
                }
            }
        }
        return enc;
    }

    private void addDefaultHost() {
        hosts.add(new Host(hostname, url, true));
    }
}
