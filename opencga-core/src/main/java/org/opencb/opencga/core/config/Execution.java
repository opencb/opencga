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
    private String batchAccount;
    private String batchKey;
    private String batchUri;
    private String batchServicePoolId;
    private String dockerImageName;
    private String dockerArgs;

    private Map<String, String> toolsPerQueue;

    public Execution() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Execution{");
        sb.append("mode='").append(mode).append('\'');
        sb.append(", defaultQueue='").append(defaultQueue).append('\'');
        sb.append(", availableQueues='").append(availableQueues).append('\'');
        sb.append(", batchAccount='").append(batchAccount).append('\'');
        sb.append(", batchKey='").append(batchKey).append('\'');
        sb.append(", batchUri='").append(batchUri).append('\'');
        sb.append(", batchServicePoolId='").append(batchServicePoolId).append('\'');
        sb.append(", dockerImageName='").append(dockerImageName).append('\'');
        sb.append(", dockerArgs='").append(dockerArgs).append('\'');
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

    public String getBatchAccount() {
        return batchAccount;
    }

    public Execution setBatchAccount(String batchAccount) {
        this.batchAccount = batchAccount;
        return this;
    }

    public String getBatchKey() {
        return batchKey;
    }

    public Execution setBatchKey(String batchKey) {
        this.batchKey = batchKey;
        return this;
    }

    public String getBatchUri() {
        return batchUri;
    }

    public String getDockerImageName() {
        return dockerImageName;
    }

    public Execution setDockerImageName(String dockerImageName) {
        this.dockerImageName = dockerImageName;
        return this;
    }

    public Execution setBatchUri(String batchUri) {
        this.batchUri = batchUri;

        return this;
    }

    public String getBatchServicePoolId() {
        return batchServicePoolId;
    }

    public Execution setBatchServicePoolId(String batchServicePoolId) {
        this.batchServicePoolId = batchServicePoolId;
        return this;
    }

    public String getDockerArgs() {
        return dockerArgs;
    }

    public Execution setDockerArgs(String dockerArgs) {
        this.dockerArgs = dockerArgs;
        return this;
    }
}
