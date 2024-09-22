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

package org.opencb.opencga.core.models.job;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.common.Enums;

import static org.opencb.opencga.core.tools.annotations.Tool.Scope;
import static org.opencb.opencga.core.tools.annotations.Tool.Type;

public class ToolInfo {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "scope", description = FieldConstants.TOOL_INFO_SCOPE_DESCRIPTION)
    private Scope scope;

    @DataField(id = "type", description = FieldConstants.TOOL_INFO_TYPE_DESCRIPTION)
    private Type type;

    @DataField(id = "resource", description = FieldConstants.TOOL_INFO_RESOURCE_DESCRIPTION)
    private Enums.Resource resource;

    @DataField(id = "resource", description = FieldConstants.TOOL_INFO_EXTERNAL_EXECUTOR_DESCRIPTION)
    private ToolInfoExecutor externalExecutor;


    public ToolInfo() {
    }

    public ToolInfo(String id, String description, Scope scope, Type type, Enums.Resource resource) {
        this(id, description, scope, type, resource, null);
    }

    public ToolInfo(String id, String description, Scope scope, Type type, Enums.Resource resource, ToolInfoExecutor externalExecutor) {
        this.id = id;
        this.description = description;
        this.scope = scope;
        this.type = type;
        this.resource = resource;
        this.externalExecutor = externalExecutor;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ToolInfo{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", scope=").append(scope);
        sb.append(", type=").append(type);
        sb.append(", resource=").append(resource);
        sb.append(", externalExecutor=").append(externalExecutor);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ToolInfo setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ToolInfo setDescription(String description) {
        this.description = description;
        return this;
    }

    public Scope getScope() {
        return scope;
    }

    public ToolInfo setScope(Scope scope) {
        this.scope = scope;
        return this;
    }

    public Type getType() {
        return type;
    }

    public ToolInfo setType(Type type) {
        this.type = type;
        return this;
    }

    public Enums.Resource getResource() {
        return resource;
    }

    public ToolInfo setResource(Enums.Resource resource) {
        this.resource = resource;
        return this;
    }

    public ToolInfoExecutor getExternalExecutor() {
        return externalExecutor;
    }

    public ToolInfo setExternalExecutor(ToolInfoExecutor externalExecutor) {
        this.externalExecutor = externalExecutor;
        return this;
    }
}
