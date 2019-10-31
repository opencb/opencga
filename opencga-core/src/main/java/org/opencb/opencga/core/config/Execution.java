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

import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 18/04/16.
 */
public class Execution {

    private String mode;
    private String defaultQueue;
    private String availableQueues;
    private int maxConcurrentIndexJobs;
    private Map<String, String> toolsPerQueue;
    private Map<String, String> options;
    private String k8sMasterNode;
    private String imageName;
    private String namespace;
    private String cpu;
    private String memory;
    private List<K8SVolumesMount> k8SVolumesMount;

    public Execution() {
    }

    @Override
    public String toString() {
        return "Execution{" +
                "mode='" + mode + '\'' +
                ", defaultQueue='" + defaultQueue + '\'' +
                ", availableQueues='" + availableQueues + '\'' +
                ", maxConcurrentIndexJobs=" + maxConcurrentIndexJobs +
                ", toolsPerQueue=" + toolsPerQueue +
                ", options=" + options +
                ", k8sMasterNode='" + k8sMasterNode + '\'' +
                ", imageName='" + imageName + '\'' +
                ", namespace='" + namespace + '\'' +
                ", cpu='" + cpu + '\'' +
                ", memory='" + memory + '\'' +
                ", k8SVolumesMount=" + k8SVolumesMount +
                '}';
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

    public int getMaxConcurrentIndexJobs() {
        return maxConcurrentIndexJobs;
    }

    public Execution setMaxConcurrentIndexJobs(int maxConcurrentIndexJobs) {
        this.maxConcurrentIndexJobs = maxConcurrentIndexJobs;
        return this;
    }

    public Map<String, String> getToolsPerQueue() {
        return toolsPerQueue;
    }

    public Execution setToolsPerQueue(Map<String, String> toolsPerQueue) {
        this.toolsPerQueue = toolsPerQueue;
        return this;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public Execution setOptions(Map<String, String> options) {
        this.options = options;
        return this;
    }

    public String getK8sMasterNode() {
        return k8sMasterNode;
    }

    public void setK8sMasterNode(String k8sMasterNode) {
        this.k8sMasterNode = k8sMasterNode;
    }

    public String getImageName() {
        return imageName;
    }

    public void setImageName(String imageName) {
        this.imageName = imageName;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }


    public List<K8SVolumesMount> getK8SVolumesMount() {
        return k8SVolumesMount;
    }

    public void setK8SVolumesMount(List<K8SVolumesMount> k8SVolumesMount) {
        this.k8SVolumesMount = k8SVolumesMount;
    }

    public String getCpu() {
        return cpu;
    }

    public Execution setCpu(String cpu) {
        this.cpu = cpu;
        return this;
    }

    public String getMemory() {
        return memory;
    }

    public Execution setMemory(String memory) {
        this.memory = memory;
        return this;
    }
}
