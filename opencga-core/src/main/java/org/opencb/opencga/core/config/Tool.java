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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Tool {

    private String dockerId;
    private String params;
    private Map<String, String> resources;

    public Tool() {
        resources = new HashMap<>();
    }

    public Tool(String dockerId, String params, Map<String, String> resources) {
        this.dockerId = dockerId;
        this.params = params;
        this.resources = resources;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Tool{");
        sb.append("dockerId='").append(dockerId).append('\'');
        sb.append(", params='").append(params).append('\'');
        sb.append(", resources=").append(resources);
        sb.append('}');
        return sb.toString();
    }

    public String getDockerId() {
        return dockerId;
    }

    public Tool setDockerId(String dockerId) {
        this.dockerId = dockerId;
        return this;
    }

    public String getParams() {
        return params;
    }

    public Tool setParams(String params) {
        this.params = params;
        return this;
    }

    public Map<String, String> getResources() {
        return resources;
    }

    public Tool setResources(Map<String, String> resources) {
        this.resources = resources;
        return this;
    }
}
