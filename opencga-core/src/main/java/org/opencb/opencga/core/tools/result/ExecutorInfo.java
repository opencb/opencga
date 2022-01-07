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
import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

public class ExecutorInfo {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    @DataField(id = "clazz", indexed = true,
            description = FieldConstants.EXECUTION_INFO_CLASS_DESCRIPTION)
    @JsonProperty("class")
    private String clazz;

    @DataField(id = "params", indexed = true,
            description = FieldConstants.EXECUTION_INFO_PARAMS_DESCRIPTION)
    private ObjectMap params;

    @DataField(id = "source", indexed = true,
            description = FieldConstants.EXECUTION_INFO_SOURCE_DESCRIPTION)
    private ToolExecutor.Source source;

    @DataField(id = "framework", indexed = true,
            description = FieldConstants.EXECUTION_INFO_FRAMEWORK_DESCRIPTION)
    private ToolExecutor.Framework framework;

    public ExecutorInfo() {
    }

    public ExecutorInfo(String id,
                        Class<? extends OpenCgaToolExecutor> clazz,
                        ObjectMap params,
                        ToolExecutor.Source source,
                        ToolExecutor.Framework framework) {
        this.id = id;
        this.clazz = clazz.toString();
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

    public String getClazz() {
        return clazz;
    }

    public ExecutorInfo setClazz(String clazz) {
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
