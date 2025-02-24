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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Resource {

    private String baseUrl;
    private Path basePath;
    private List<String> fetchOnInit;
    private List<ResourceFile> files;

    public Resource() {
        this.fetchOnInit = new ArrayList<>();
        this.files = new ArrayList<>();
    }

    public Resource(String baseUrl, Path basePath, List<String> fetchOnInit, List<ResourceFile> files) {
        this.baseUrl = baseUrl;
        this.basePath = basePath;
        this.fetchOnInit = fetchOnInit;
        this.files = files;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Resource{");
        sb.append("baseUrl='").append(baseUrl).append('\'');
        sb.append(", basePath=").append(basePath);
        sb.append(", fetchOnInit=").append(fetchOnInit);
        sb.append(", files=").append(files);
        sb.append('}');
        return sb.toString();
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public Resource setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
        return this;
    }

    public Path getBasePath() {
        return basePath;
    }

    public Resource setBasePath(Path basePath) {
        this.basePath = basePath;
        return this;
    }

    public List<String> getFetchOnInit() {
        return fetchOnInit;
    }

    public Resource setFetchOnInit(List<String> fetchOnInit) {
        this.fetchOnInit = fetchOnInit;
        return this;
    }

    public List<ResourceFile> getFiles() {
        return files;
    }

    public Resource setFiles(List<ResourceFile> files) {
        this.files = files;
        return this;
    }
}
