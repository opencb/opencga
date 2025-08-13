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

import static org.opencb.opencga.core.config.Configuration.reportUnusedField;

public class Analysis {

    public static final String BASE_DOCKER_KEY = "ext-tools";
    public static final String GENOMICS_DOCKER_KEY = "genomics";
    public static final String TRASNSCRIPTOMICS_DOCKER_KEY = "transcriptomics";

    private List<String> packages;
    private WorkflowConfiguration workflow;

    private String scratchDir;

    @Deprecated
    private String resourceUrl;

    private Resource resource;

    @Deprecated
    private String opencgaExtTools;

    private Map<String, String> opencgaDockers;

    private List<AnalysisTool> tools;

    private Execution execution;

    private List<FrameworkConfiguration> frameworks;

    public Analysis() {
        this.workflow = new WorkflowConfiguration();
        this.packages = new ArrayList<>();
        this.resource = new Resource();
        this.opencgaDockers = new HashMap<>();
        this.tools = new ArrayList<>();
        this.execution = new Execution();
        this.frameworks = new ArrayList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Analysis{");
        sb.append("packages=").append(packages);
        sb.append(", scratchDir='").append(scratchDir).append('\'');
        sb.append(", resource=").append(resource);
        sb.append(", opencgaDockers='").append(opencgaDockers).append('\'');
        sb.append(", tools=").append(tools);
        sb.append(", execution=").append(execution);
        sb.append(", frameworks=").append(frameworks);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getPackages() {
        return packages;
    }

    public Analysis setPackages(List<String> packages) {
        this.packages = packages;
        return this;
    }

    public WorkflowConfiguration getWorkflow() {
        return workflow;
    }

    public Analysis setWorkflow(WorkflowConfiguration workflow) {
        this.workflow = workflow;
        return this;
    }

    public String getScratchDir() {
        return scratchDir;
    }

    public Analysis setScratchDir(String scratchDir) {
        this.scratchDir = scratchDir;
        return this;
    }

    @Deprecated
    public String getResourceUrl() {
        return (resource != null) ? this.resource.getBaseUrl() : null;
    }

    @Deprecated
    public Analysis setResourceUrl(String resourceUrl) {
        reportUnusedField("configuration.yml#analysis.resourceUrl", resourceUrl);
        this.resourceUrl = resourceUrl;
        return this;
    }

    public Resource getResource() {
        return resource;
    }

    public Analysis setResource(Resource resource) {
        this.resource = resource;
        return this;
    }

    @Deprecated
    public String getOpencgaExtTools() {
        return opencgaDockers.get(BASE_DOCKER_KEY);
    }

    @Deprecated
    public Analysis setOpencgaExtTools(String opencgaExtTools) {
        this.opencgaDockers.put(BASE_DOCKER_KEY, opencgaExtTools);
        return this;
    }

    public Map<String, String> getOpencgaDockers() {
        return opencgaDockers;
    }

    public Analysis setOpencgaDockers(Map<String, String> opencgaDockers) {
        this.opencgaDockers = opencgaDockers;
        return this;
    }

    public List<AnalysisTool> getTools() {
        return tools;
    }

    public Analysis setTools(List<AnalysisTool> tools) {
        this.tools = tools;
        return this;
    }

    public Execution getExecution() {
        return execution;
    }

    public Analysis setExecution(Execution execution) {
        this.execution = execution;
        return this;
    }

    public List<FrameworkConfiguration> getFrameworks() {
        return frameworks;
    }

    public Analysis setFrameworks(List<FrameworkConfiguration> frameworks) {
        this.frameworks = frameworks;
        return this;
    }

}
