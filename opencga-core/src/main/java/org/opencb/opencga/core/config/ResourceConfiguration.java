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

public class ResourceConfiguration {

    private String baseUrl;
    private List<String> fetchOnInit;

    private Map<String, String> files;

    public ResourceConfiguration() {
        this.fetchOnInit = new ArrayList<>();
        this.files = new HashMap<>();
    }

    public ResourceConfiguration(String baseUrl, List<String> fetchOnInit, Map<String, String> files) {
        this.baseUrl = baseUrl;
        this.fetchOnInit = fetchOnInit;
        this.files = files;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ResourceConfiguration{");
        sb.append("baseUrl='").append(baseUrl).append('\'');
        sb.append(", fetchOnInit=").append(fetchOnInit);
        sb.append(", files=").append(files);
        sb.append('}');
        return sb.toString();
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public ResourceConfiguration setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
        return this;
    }

    public List<String> getFetchOnInit() {
        return fetchOnInit;
    }

    public ResourceConfiguration setFetchOnInit(List<String> fetchOnInit) {
        this.fetchOnInit = fetchOnInit;
        return this;
    }

    public Map<String, String> getFiles() {
        return files;
    }

    public ResourceConfiguration setFiles(Map<String, String> files) {
        this.files = files;
        return this;
    }
}
