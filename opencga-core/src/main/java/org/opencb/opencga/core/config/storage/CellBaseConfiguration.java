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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang3.StringUtils;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Created by imedina on 04/05/15.
 */
@JsonIgnoreProperties(allowSetters = true, value = {"host", "preferred", "hosts", "database"})
public class CellBaseConfiguration {
    /*
     * URL to CellBase REST web services, by default official UCam installation is used
     */
    private String url;

    /*
     * CellBase version to be used, by default the 'v4' stable
     */
    private String version;

    private static final String CELLBASE_HOST = "http://ws.opencb.org/cellbase/";
    private static final String CELLBASE_VERSION = "v4";

    public CellBaseConfiguration() {
        this(CELLBASE_HOST, CELLBASE_VERSION);
    }

    public CellBaseConfiguration(String url, String version) {
        this.url = url;
        this.version = version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CellBaseConfiguration{");
        sb.append("url=").append(url);
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getUrl() {
        return url;
    }

    public CellBaseConfiguration setUrl(String url) {
        this.url = url;
        return this;
    }

    @Deprecated
    public String getHost() {
        return url;
    }

    @Deprecated
    public CellBaseConfiguration setHost(String host) {
        if (host != null) {
            LoggerFactory.getLogger(CellBaseConfiguration.class).warn("Deprecated option 'cellbase.host'. Use 'cellbase.url'");
        }
        url = host;
        return this;
    }

    @Deprecated
    public List<String> getHosts() {
        return Collections.singletonList(url);
    }

    @Deprecated
    public void setHosts(List<String> hosts) {
        if (hosts != null) {
            LoggerFactory.getLogger(CellBaseConfiguration.class).warn("Deprecated option 'cellbase.hosts'. Use 'cellbase.url'");
        }
        if (hosts == null || hosts.isEmpty()) {
            url = null;
        } else {
            if (hosts.size() != 1) {
                throw new IllegalArgumentException("Unsupported multiple cellbase hosts");
            }
            url = hosts.get(0);
        }
    }

    public String getVersion() {
        return version;
    }

    public CellBaseConfiguration setVersion(String version) {
        this.version = version;
        return this;
    }

    @Deprecated
    public Object getDatabase() {
        return null;
    }

    @Deprecated
    public void setDatabase(Object database) {
        if (database != null) {
            LoggerFactory.getLogger(CellBaseConfiguration.class).warn("Deprecated option 'storage-configuration.yml#cellbase.database'");
        }
    }

    @Deprecated
    public String getPreferred() {
        return "";
    }

    @Deprecated
    public void setPreferred(String preferred) {
        if (StringUtils.isNotEmpty(preferred)) {
            LoggerFactory.getLogger(CellBaseConfiguration.class).warn("Deprecated option 'storage-configuration.yml#cellbase.preferred'");
        }
    }

    public ClientConfiguration toClientConfiguration() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setVersion(this.getVersion());
        clientConfiguration.setDefaultSpecies("hsapiens");
        RestConfig rest = new RestConfig();
        rest.setHosts(Collections.singletonList(this.getUrl().replace("/webservices/rest", "")));
        clientConfiguration.setRest(rest);

        return clientConfiguration;
    }
}
