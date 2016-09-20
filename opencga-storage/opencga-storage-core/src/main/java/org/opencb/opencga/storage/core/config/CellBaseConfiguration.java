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

import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by imedina on 04/05/15.
 */
public class CellBaseConfiguration {

    /*
     * URL to CellBase REST web services, by default official UCam installation is used
     */
    private List<String> hosts;

    /*
     * CellBase version to be used, by default the 'latest' stable
     */
    private String version;

    private DatabaseCredentials database;

    /*
     * This can be 'remote' or 'local'
     */
    private String preferred;

    private static final String CELLBASE_HOST = "http://bioinfo.hpc.cam.ac.uk/cellbase/webservices/rest/";
    private static final String CELLBASE_VERSION = "latest";

    public CellBaseConfiguration() {
        this(Arrays.asList(CELLBASE_HOST), CELLBASE_VERSION, new DatabaseCredentials());
    }

    public CellBaseConfiguration(List<String> hosts, String version, DatabaseCredentials database) {
        this.hosts = hosts;
        this.version = version;
        this.database = database;

        this.preferred = "local";
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CellBaseConfiguration{");
        sb.append("hosts=").append(hosts);
        sb.append(", version='").append(version).append('\'');
        sb.append(", database=").append(database);
        sb.append(", preferred='").append(preferred).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public DatabaseCredentials getDatabase() {
        return database;
    }

    public void setDatabase(DatabaseCredentials database) {
        this.database = database;
    }

    public String getPreferred() {
        return preferred;
    }

    public void setPreferred(String preferred) {
        this.preferred = preferred;
    }

    public ClientConfiguration toClientConfiguration() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setVersion(this.getVersion());
        clientConfiguration.setDefaultSpecies("hsapiens");
        RestConfig rest = new RestConfig();
        List<String> hosts = new ArrayList<>(this.getHosts().size());
        for (String host : this.getHosts()) {
            hosts.add(host.replace("/webservices/rest", ""));
        }
        rest.setHosts(hosts);
        clientConfiguration.setRest(rest);
        return clientConfiguration;
    }
}
