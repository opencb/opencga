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

import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 07/09/15.
 */
public class ServerConfiguration {

    private int rest;
    private int grpc;
    private String authManager;
    private String storageEngine;
    private List<String> authorizedHosts;
    private Map<String, String> options;

    public ServerConfiguration() {
    }

    public ServerConfiguration(int rest, int grpc, String storageEngine, List<String> authorizedHosts, Map<String, String> options) {
        this.rest = rest;
        this.grpc = grpc;
        this.storageEngine = storageEngine;
        this.authorizedHosts = authorizedHosts;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ServerConfiguration{");
        sb.append("rest=").append(rest);
        sb.append(", grpc=").append(grpc);
        sb.append(", storageEngine='").append(storageEngine).append('\'');
        sb.append(", authorizedHosts=").append(authorizedHosts);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public int getRest() {
        return rest;
    }

    public void setRest(int rest) {
        this.rest = rest;
    }

    public int getGrpc() {
        return grpc;
    }

    public void setGrpc(int grpc) {
        this.grpc = grpc;
    }

    public String getAuthManager() {
        return authManager;
    }

    public void setAuthManager(String authManager) {
        this.authManager = authManager;
    }

    public String getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(String storageEngine) {
        this.storageEngine = storageEngine;
    }

    public List<String> getAuthorizedHosts() {
        return authorizedHosts;
    }

    public void setAuthorizedHosts(List<String> authorizedHosts) {
        this.authorizedHosts = authorizedHosts;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

}
