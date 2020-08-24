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

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 18/04/16.
 */
public class Execution {

    private String id;
    private String defaultQueue;
    private String availableQueues;
    private Map<String, List<String>> toolsPerQueue;
    private Map<String, Integer> maxConcurrentJobs;
    private ObjectMap options;

    public Execution() {
        toolsPerQueue = new HashMap<>();
        options = new ObjectMap();
        maxConcurrentJobs = new HashMap<>();
    }

    public String getId() {
        return id;
    }

    public Execution setId(String id) {
        this.id = id;
        return this;
    }

    public String getDefaultQueue() {
        return defaultQueue;
    }

    public Execution setDefaultQueue(String defaultQueue) {
        this.defaultQueue = defaultQueue;
        return this;
    }

    public String getAvailableQueues() {
        return availableQueues;
    }

    public Execution setAvailableQueues(String availableQueues) {
        this.availableQueues = availableQueues;
        return this;
    }

    public Map<String, List<String>> getToolsPerQueue() {
        return toolsPerQueue;
    }

    public Execution setToolsPerQueue(Map<String, List<String>> toolsPerQueue) {
        this.toolsPerQueue = toolsPerQueue;
        return this;
    }

    public Map<String, Integer> getMaxConcurrentJobs() {
        return maxConcurrentJobs;
    }

    public Execution setMaxConcurrentJobs(Map<String, Integer> maxConcurrentJobs) {
        this.maxConcurrentJobs = maxConcurrentJobs;
        return this;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public Execution setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Execution{");
        sb.append("id='").append(id).append('\'');
        sb.append(", defaultQueue='").append(defaultQueue).append('\'');
        sb.append(", availableQueues='").append(availableQueues).append('\'');
        sb.append(", toolsPerQueue=").append(toolsPerQueue);
        sb.append(", maxConcurrentJobs=").append(maxConcurrentJobs);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }
}
