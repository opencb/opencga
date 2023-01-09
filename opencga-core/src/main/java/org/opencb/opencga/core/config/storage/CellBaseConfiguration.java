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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang3.StringUtils;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Created by imedina on 04/05/15.
 */
@JsonIgnoreProperties(allowSetters = true, value = {"host", "preferred", "hosts", "database"})
public class CellBaseConfiguration {

    @DataField(id = "url", description = "URL to CellBase REST web services, by default official ZettaGenomics installation is used")
    private String url;

    @DataField(id = "version", description = "URL to CellBase REST web services, by default official ZettaGenomics installation is used")
    private String version;

    @DataField(id = "version", description = "CellBase data release version to be used. If empty, will use the latest")
    private String dataRelease;

    public CellBaseConfiguration() {
        this(ParamConstants.CELLBASE_URL, ParamConstants.CELLBASE_VERSION);
    }

    public CellBaseConfiguration(String url, String version) {
        this.url = url;
        this.version = version;
    }

    public CellBaseConfiguration(String url, String version, String dataRelease) {
        this.url = url;
        this.version = version;
        this.dataRelease = dataRelease;
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

    public String getDataRelease() {
        return dataRelease;
    }

    public CellBaseConfiguration setDataRelease(String dataRelease) {
        this.dataRelease = dataRelease;
        return this;
    }

//    @Deprecated
//    public String getHost() {
//        return url;
//    }
//
//    @Deprecated
//    public CellBaseConfiguration setHost(String host) {
//        if (host != null) {
//            LoggerFactory.getLogger(CellBaseConfiguration.class).warn("Deprecated option 'cellbase.host'. Use 'cellbase.url'");
//        }
//        url = host;
//        return this;
//    }

//    @Deprecated
//    public List<String> getHosts() {
//        return Collections.singletonList(url);
//    }
//
//    @Deprecated
//    public CellBaseConfiguration setHosts(List<String> hosts) {
//        if (hosts != null) {
//            LoggerFactory.getLogger(CellBaseConfiguration.class).warn("Deprecated option 'cellbase.hosts'. Use 'cellbase.url'");
//        }
//        if (hosts == null || hosts.isEmpty()) {
//            url = null;
//        } else {
//            if (hosts.size() != 1) {
//                throw new IllegalArgumentException("Unsupported multiple cellbase hosts");
//            }
//            url = hosts.get(0);
//        }
//        return this;
//    }

    public String getVersion() {
        return version;
    }

    public CellBaseConfiguration setVersion(String version) {
        this.version = version;
        return this;
    }

    @Deprecated
    @JsonIgnore
    public Object getDatabase() {
        return null;
    }

    @Deprecated
    @JsonIgnore
    public CellBaseConfiguration setDatabase(Object database) {
        if (database != null) {
            LoggerFactory.getLogger(CellBaseConfiguration.class).warn("Deprecated option 'storage-configuration.yml#cellbase.database'");
        }
        return this;
    }

    @Deprecated
    @JsonIgnore
    public String getPreferred() {
        return "";
    }

    @Deprecated
    @JsonIgnore
    public CellBaseConfiguration setPreferred(String preferred) {
        if (StringUtils.isNotEmpty(preferred)) {
            LoggerFactory.getLogger(CellBaseConfiguration.class).warn("Deprecated option 'storage-configuration.yml#cellbase.preferred'");
        }
        return this;
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
