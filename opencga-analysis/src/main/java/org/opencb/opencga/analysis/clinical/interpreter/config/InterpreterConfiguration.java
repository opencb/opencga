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

package org.opencb.opencga.analysis.clinical.interpreter.config;

import java.util.List;
import java.util.Map;

public class InterpreterConfiguration {

    private String id;
    private String name;
    private String description;
    private String version;
    private Map<String, Object> inputs;
    private List<ExecutionInterpreterConfiguration> executions;
    private QueryInterpreterConfiguration queries;


    public InterpreterConfiguration() {
    }

    public InterpreterConfiguration(String id, String name, String description, String version, Map<String, Object> inputs,
                                    List<ExecutionInterpreterConfiguration> executions, QueryInterpreterConfiguration queries) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.version = version;
        this.inputs = inputs;
        this.executions = executions;
        this.queries = queries;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpreterConfiguration{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", inputs=").append(inputs);
        sb.append(", executions=").append(executions);
        sb.append(", queries=").append(queries);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public InterpreterConfiguration setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public InterpreterConfiguration setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public InterpreterConfiguration setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public InterpreterConfiguration setVersion(String version) {
        this.version = version;
        return this;
    }

    public Map<String, Object> getInputs() {
        return inputs;
    }

    public InterpreterConfiguration setInputs(Map<String, Object> inputs) {
        this.inputs = inputs;
        return this;
    }

    public List<ExecutionInterpreterConfiguration> getExecutions() {
        return executions;
    }

    public InterpreterConfiguration setExecutions(List<ExecutionInterpreterConfiguration> executions) {
        this.executions = executions;
        return this;
    }

    public QueryInterpreterConfiguration getQueries() {
        return queries;
    }

    public InterpreterConfiguration setQueries(QueryInterpreterConfiguration queries) {
        this.queries = queries;
        return this;
    }
}
