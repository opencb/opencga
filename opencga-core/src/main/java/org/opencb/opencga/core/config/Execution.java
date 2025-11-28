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

import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.*;

import static org.opencb.opencga.core.config.Configuration.reportUnusedField;

/**
 * Created by imedina on 18/04/16.
 */
public class Execution {

    public static final String JOBS_REUSE_ENABLED = "jobs.reuse.enabled";
    public static final boolean JOBS_REUSE_ENABLED_DEFAULT = true;
    public static final String JOBS_REUSE_TOOLS = "jobs.reuse.tools";
    public static final List<String> JOBS_REUSE_TOOLS_DEFAULT = Arrays.asList(
            "variant-index",
            "variant-stats-index",
            "variant-annotation-index",
            "variant-secondary-annotation-index",
            "variant-secondary-sample-index"
    );

    @Deprecated
    @DataField(id = "id", deprecated = true, description = "Use queues.id instead")
    private String id;

    @Deprecated
    @DataField(id = "defaultQueue", deprecated = true, description = "Use queues instead")
    private String defaultQueue;

    @Deprecated
    @DataField(id = "availableQueues", deprecated = true, description = "Use queues instead")
    private String availableQueues;

    @DataField(id = "queues", description = "List of execution queues")
    private List<ExecutionQueue> queues;

    @DataField(id = "defaultRequest", description = "Default request values for new executions.")
    private ExecutionRequest defaultRequest;

    @DataField(id = "requestFactor", description = "Execution request factor to be applied. This is to take into account that systems "
            + "always reserve some resources for the system itself, so the request factor is used to scale down the number of resources "
            + "requested by the user so that multiple jobs can run in parallel without overloading the system.")
    private ExecutionFactor requestFactor;

    @Deprecated
    private Map<String, List<String>> toolsPerQueue;
    private Map<String, Integer> maxConcurrentJobs;
    private ObjectMap options;

    public Execution() {
        toolsPerQueue = new HashMap<>();
        options = new ObjectMap();
        maxConcurrentJobs = new HashMap<>();
        queues = new ArrayList<>();
    }

    @Deprecated
    public String getId() {
        return id;
    }

    @Deprecated
    public Execution setId(String id) {
        reportUnusedField("configuration.yml#analysis.execution.id", availableQueues, "The option was deprecated and a list of "
                + "configuration.yml#analysis.execution.queues must now be used instead.");
        return this;
    }

    @Deprecated
    public String getDefaultQueue() {
        return defaultQueue;
    }

    @Deprecated
    public Execution setDefaultQueue(String defaultQueue) {
        reportUnusedField("configuration.yml#analysis.execution.defaultQueue", availableQueues, "The option was deprecated and a list of "
                + "configuration.yml#analysis.execution.queues must now be used instead.");
        return this;
    }

    @Deprecated
    public String getAvailableQueues() {
        return availableQueues;
    }

    @Deprecated
    public Execution setAvailableQueues(String availableQueues) {
        reportUnusedField("configuration.yml#analysis.execution.availableQueues", availableQueues, "The option was deprecated and a list of"
                + " configuration.yml#analysis.execution.queues must now be used instead.");
        return this;
    }

    public List<ExecutionQueue> getQueues() {
        return queues;
    }

    public Execution setQueues(List<ExecutionQueue> queues) {
        this.queues = queues;
        return this;
    }

    public ExecutionRequest getDefaultRequest() {
        return defaultRequest;
    }

    public Execution setDefaultRequest(ExecutionRequest defaultRequest) {
        this.defaultRequest = defaultRequest;
        return this;
    }

    public ExecutionFactor getRequestFactor() {
        return requestFactor;
    }

    public Execution setRequestFactor(ExecutionFactor requestFactor) {
        this.requestFactor = requestFactor;
        return this;
    }

    @Deprecated
    public Map<String, List<String>> getToolsPerQueue() {
        return toolsPerQueue;
    }

    @Deprecated
    public Execution setToolsPerQueue(Map<String, List<String>> toolsPerQueue) {
        reportUnusedField("configuration.yml#analysis.execution.toolsPerQueue", toolsPerQueue, "The option was deprecated and a list of "
                + "configuration.yml#analysis.execution.queues must now be used instead.");
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
        sb.append(", queues='").append(queues).append('\'');
        sb.append(", toolsPerQueue=").append(toolsPerQueue);
        sb.append(", maxConcurrentJobs=").append(maxConcurrentJobs);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }
}
