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

    private List<Host> hosts;
    private boolean tokenAutoRefresh;
    private boolean tlsAllowInvalidCertificates;
    private int timeout;
    private Host currentHost;
    private QueryRestConfig query;

    public RestConfig() {

    }

    public RestConfig(boolean tokenAutoRefresh, QueryRestConfig query, List<Host> hosts) {
        this.hosts = hosts;
        this.tokenAutoRefresh = tokenAutoRefresh;
        this.query = query;
        if (hosts != null && !hosts.isEmpty()) {
            currentHost = hosts.get(0);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RestConfig{");
        sb.append("hosts=").append(hosts);
        sb.append(", tokenAutoRefresh=").append(tokenAutoRefresh);
        sb.append(", tlsAllowInvalidCertificates=").append(tlsAllowInvalidCertificates);
        sb.append(", timeout=").append(timeout);
        sb.append(", currentHost=").append(currentHost);
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

    private Host getHostByUrl(String s) {
        for (Host host : hosts) {
            if (host.getUrl().equals(s)) {
                return host;
            }
        }
        return null;
    }

    private Host getHostByName(String s) {
        for (Host host : hosts) {
            if (host.getName().equals(s)) {
                return host;
            }
        }
        return null;
    }

    private boolean existsUrl(String s) {
        for (Host host : hosts) {
            if (host.getUrl().equals(s)) {
                return true;
            }
        }
        return false;
    }

    public boolean existsName(String s) {
        for (Host host : hosts) {
            if (host.getName().equals(s)) {
                return true;
            }
        }
        return false;
    }

    public String getCurrentHostname() {
        if (currentHost == null) {
            if (hosts != null && !hosts.isEmpty()) {
                currentHost = hosts.get(0);
            } else {
                return "";
            }
        }
        return currentHost.getName();
    }

    public String getCurrentUrl() {
        if (currentHost == null) {
            if (hosts != null && !hosts.isEmpty()) {
                currentHost = hosts.get(0);
            } else {
                return "";
            }
        }
        return currentHost.getUrl();
    }

    public void setCurrentHostname(String name) {

        if (!existsName(name)) {
            currentHost = new Host(name, name, true);
        } else {
            currentHost = getHostByName(name);
        }
        hosts.add(0, currentHost);
    }

    public void setCurrentUrl(String url) {
        if (!existsUrl(url)) {
            currentHost = new Host(url, url, true);
        } else {
            currentHost = getHostByUrl(url);
        }
        hosts.add(0, currentHost);
    }
}
