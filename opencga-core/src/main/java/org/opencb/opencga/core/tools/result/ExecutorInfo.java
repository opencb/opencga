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

package org.opencb.opencga.core.tools.result;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

public class ExecutorInfo {

    private String id;
    @JsonProperty("class")
    private Class<? extends OpenCgaToolExecutor> clazz;
    private ObjectMap params;
    private ToolExecutor.Source source;
    private ToolExecutor.Framework framework;

    public ExecutorInfo() {
    }

    public ExecutorInfo(String id,
                        Class<? extends OpenCgaToolExecutor> clazz,
                        ObjectMap params,
                        ToolExecutor.Source source,
                        ToolExecutor.Framework framework) {
        this.id = id;
        this.clazz = clazz;
        this.params = params;
        this.source = source;
        this.framework = framework;
    }

    public String getId() {
        return id;
    }

    public ExecutorInfo setId(String id) {
        this.id = id;
        return this;
    }

    public Class<? extends OpenCgaToolExecutor> getClazz() {
        return clazz;
    }

    public ExecutorInfo setClazz(Class<? extends OpenCgaToolExecutor> clazz) {
        this.clazz = clazz;
        return this;
    }

    public ObjectMap getParams() {
        return params;
    }

    public ExecutorInfo setParams(ObjectMap params) {
        this.params = params;
        return this;
    }

    public ToolExecutor.Source getSource() {
        return source;
    }

    public ExecutorInfo setSource(ToolExecutor.Source source) {
        this.source = source;
        return this;
    }

    public ToolExecutor.Framework getFramework() {
        return framework;
    }

    public ExecutorInfo setFramework(ToolExecutor.Framework framework) {
        this.framework = framework;
        return this;
    }
}
