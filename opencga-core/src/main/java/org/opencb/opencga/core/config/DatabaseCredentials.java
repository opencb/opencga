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
package org.opencb.opencga.core.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseCredentials {

    /**
     * host attribute includes port with this format 'host[:port]'.
     */
    private List<String> hosts;

    private String user;
    private String password;

    /**
     * options parameter defines database-specific parameters such as --authenticateDatabase of MongoDB.
     */
    private Map<String, String> options;


    public DatabaseCredentials() {

    }

    public DatabaseCredentials(List<String> hosts, String user, String password) {
        this(hosts, user, password, new HashMap<>());
    }

    public DatabaseCredentials(DatabaseCredentials credentials) {
        this.hosts = credentials.hosts;
        this.user = credentials.user;
        this.password = credentials.password;
        this.options = credentials.options;
    }

    public DatabaseCredentials(List<String> hosts, String user, String password, Map<String, String> options) {
        this.hosts = hosts;
        this.user = user;
        this.password = password;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DatabaseCredentials{");
        sb.append("hosts=").append(hosts);
        sb.append(", user='").append(user).append('\'');
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getHosts() {
        return hosts;
    }

    public DatabaseCredentials setHosts(List<String> hosts) {
        this.hosts = hosts == null
                ? java.util.Collections.emptyList()
                : hosts.stream().flatMap(s -> java.util.Arrays.stream(s.split(","))).collect(java.util.stream.Collectors.toList());
        return this;
    }

    public String getUser() {
        return user;
    }

    public DatabaseCredentials setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public DatabaseCredentials setPassword(String password) {
        this.password = password;
        return this;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public DatabaseCredentials setOptions(Map<String, String> options) {
        this.options = options;
        return this;
    }

}
