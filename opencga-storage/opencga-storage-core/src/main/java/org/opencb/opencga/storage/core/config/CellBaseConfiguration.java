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

import java.util.Map;

/**
 * Created by imedina on 04/05/15.
 */
public class CellBaseConfiguration {

    /**
     * URL to CellBase REST web services, by default official UCam installation is used
     */
    private String host = "http://bioinfo.hpc.cam.ac.uk/cellbase/webservices/rest/";

    /**
     * Port used by the CellBase server, by default '80' is used
     */
    private int port = 80;

    /**
     * CellBase version to be used, by default the 'latest' stable
     */
    private String version = "latest";

    /**
     * options parameter defines database-specific parameters
     */
    private Map<String, String> options;

    public CellBaseConfiguration () {

    }

    public CellBaseConfiguration (String host, int port, String version, Map<String, String> options) {
        this.host = host;
        this.port = port;
        this.version = version;
        this.options = options;
    }

    @Override
    public String toString() {
        return "CellBaseConfiguration{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", version='" + version + '\'' +
                ", options=" + options +
                '}';
    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }
}
