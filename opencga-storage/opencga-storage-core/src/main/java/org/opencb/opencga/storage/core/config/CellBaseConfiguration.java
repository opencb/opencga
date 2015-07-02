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

import java.util.*;

/**
 * Created by imedina on 04/05/15.
 */
public class CellBaseConfiguration {

    /**
     * URL to CellBase REST web services, by default official UCam installation is used
     */
    private List<String> hosts;

    /**
     * CellBase version to be used, by default the 'latest' stable
     */
    private String version;

    /**
     * options parameter defines database-specific parameters
     */
//    private Map<String, String> options;

    private static final String CELLBASE_HOST= "http://bioinfo.hpc.cam.ac.uk/cellbase/webservices/rest/";
    private static final String CELLBASE_VERSION= "latest";

    public CellBaseConfiguration () {
        this(Arrays.asList(CELLBASE_HOST), CELLBASE_VERSION, new HashMap<>());
    }

    public CellBaseConfiguration(List<String> hosts, String version, Map<String, String> options) {
        this.hosts = hosts;
        this.version = version;
//        this.options = options;
    }


    @Override
    public String toString() {
        return "CellBaseConfiguration{" +
                "hosts=" + hosts +
                ", version='" + version + '\'' +
//                ", options=" + options +
                '}';
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

//    public Map<String, String> getOptions() {
//        return options;
//    }
//
//    public void setOptions(Map<String, String> options) {
//        this.options = options;
//    }

}
