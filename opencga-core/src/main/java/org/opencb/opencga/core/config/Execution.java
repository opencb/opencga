/*
 * Copyright 2015-2017 OpenCB
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

import java.util.Map;

/**
 * Created by imedina on 18/04/16.
 */
public class Execution {

    private String mode;
    private String defaultQueue;
    private String availableQueues;
    private Map<String, String> toolsPerQueue;

    public Execution() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Execution{");
        sb.append("mode='").append(mode).append('\'');
        sb.append(", defaultQueue='").append(defaultQueue).append('\'');
        sb.append(", availableQueues='").append(availableQueues).append('\'');
        sb.append(", toolsPerQueue=").append(toolsPerQueue);
        sb.append('}');
        return sb.toString();
    }

    public String getMode() {
        return mode;
    }

    public Execution setMode(String mode) {
        this.mode = mode;
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

    public Map<String, String> getToolsPerQueue() {
        return toolsPerQueue;
    }

    public Execution setToolsPerQueue(Map<String, String> toolsPerQueue) {
        this.toolsPerQueue = toolsPerQueue;
        return this;
    }
}
