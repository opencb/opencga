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

package org.opencb.opencga.storage.core.config;

import org.apache.commons.lang3.StringUtils;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Created by imedina on 04/05/15.
 */
public class CellBaseConfiguration {
    /*
     * URL to CellBase REST web services, by default official UCam installation is used
     */
    private String host;

    /*
     * CellBase version to be used, by default the 'v4' stable
     */
    private String version;

    private static final String CELLBASE_HOST = "http://bioinfo.hpc.cam.ac.uk/cellbase/";
    private static final String CELLBASE_VERSION = "v4";

    public CellBaseConfiguration() {
        this(CELLBASE_HOST, CELLBASE_VERSION);
    }

    public CellBaseConfiguration(String host, String version) {
        this.host = host;
        this.version = version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CellBaseConfiguration{");
        sb.append("host=").append(host);
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getHost() {
        return host;
    }

    public CellBaseConfiguration setHost(String host) {
        this.host = host;
        return this;
    }

    @Deprecated
    public List<String> getHosts() {
        return Collections.singletonList(host);
    }

    @Deprecated
    public void setHosts(List<String> hosts) {
        if (hosts == null || hosts.isEmpty()) {
            host = null;
        } else {
            if (hosts.size() != 1) {
                throw new IllegalArgumentException("Unsupported multiple cellbase hosts");
            }
            host = hosts.get(0);
        }
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
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
        rest.setHosts(Collections.singletonList(this.getHost().replace("/webservices/rest", "")));
        clientConfiguration.setRest(rest);

        return clientConfiguration;
    }
}
