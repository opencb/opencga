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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Analysis {

    private List<String> packages;

    private String scratchDir;

    private Execution execution;

    private List<FrameworkConfiguration> frameworks;

    public Analysis() {
        packages = new ArrayList<>();
        execution = new Execution();
        frameworks = new ArrayList<>();
    }

    public List<String> getPackages() {
        return packages;
    }

    public Analysis setPackages(List<String> packages) {
        this.packages = packages;
        return this;
    }

    public String getScratchDir() {
        return scratchDir;
    }

    public Analysis setScratchDir(String scratchDir) {
        this.scratchDir = scratchDir;
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
