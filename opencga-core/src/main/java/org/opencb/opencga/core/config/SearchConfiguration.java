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


import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by wasim on 09/11/16.
 */
public class SearchConfiguration {

    private List<String> hosts;
    private String configSet;
    private String mode;
    private String user;
    private String password;
    private String manager;
    private boolean active;
    private int timeout;
    private int writeTimeout;
    private int insertBatchSize;

    private static final String DEFAULT_MODE = "cloud";
    private static final boolean DEFAULT_ACTIVE = true;
    private static final int DEFAULT_TIMEOUT = 30000;
    private static final int DEFAULT_WRITE_TIMEOUT = 120000;
    private static final int DEFAULT_INSERT_BATCH_SIZE = 10000;


    public SearchConfiguration() {
        this(Collections.emptyList(), "", DEFAULT_MODE, "", "", "", DEFAULT_ACTIVE, DEFAULT_TIMEOUT, DEFAULT_WRITE_TIMEOUT,
                DEFAULT_INSERT_BATCH_SIZE);
    }

    @Deprecated
    public SearchConfiguration(List<String> hosts, String configSet, String mode, String user, String password, String manager,
                               boolean active, int timeout, int insertBatchSize) {
        this(hosts, configSet, mode, user, password, manager, active, timeout, DEFAULT_WRITE_TIMEOUT, insertBatchSize);
    }

    public SearchConfiguration(List<String> hosts, String configSet, String mode, String user, String password, String manager,
                               boolean active, int timeout, int writeTimeout, int insertBatchSize) {
        this.hosts = hosts;
        this.configSet = configSet;
        this.mode = mode;
        this.user = user;
        this.password = password;
        this.manager = manager;
        this.active = active;
        this.timeout = timeout;
        this.writeTimeout = writeTimeout;
        this.insertBatchSize = insertBatchSize;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SearchConfiguration{");
        sb.append("hosts=").append(hosts);
        sb.append(", configSet='").append(configSet).append('\'');
        sb.append(", mode='").append(mode).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", manager='").append(manager).append('\'');
        sb.append(", active=").append(active);
        sb.append(", timeout=").append(timeout);
        sb.append(", writeTimeout=").append(writeTimeout);
        sb.append(", insertBatchSize=").append(insertBatchSize);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getHosts() {
        return hosts;
    }

    public SearchConfiguration setHosts(List<String> hosts) {
        this.hosts = hosts;
        return this;
    }

    public String getConfigSet() {
        return configSet;
    }

    public SearchConfiguration setConfigSet(String configSet) {
        this.configSet = configSet;
        return this;
    }

    public String getMode() {
        return mode;
    }

    public SearchConfiguration setMode(String mode) {
        this.mode = mode;
        return this;
    }

    public String getUser() {
        return user;
    }

    public SearchConfiguration setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public SearchConfiguration setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getManager() {
        return manager;
    }

    public SearchConfiguration setManager(String manager) {
        this.manager = manager;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public SearchConfiguration setActive(boolean active) {
        this.active = active;
        return this;
    }

    public int getTimeout() {
        return timeout;
    }

    public SearchConfiguration setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public SearchConfiguration setWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
        return this;
    }

    public int getInsertBatchSize() {
        return insertBatchSize;
    }

    public SearchConfiguration setInsertBatchSize(int insertBatchSize) {
        this.insertBatchSize = insertBatchSize;
        return this;
    }
}
