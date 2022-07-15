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

package org.opencb.opencga.core.models.user;

import org.opencb.commons.annotations.DataField;

import java.util.Map;

public class ConfigUpdateParams {

    @DataField(required = true, description = "Config id (Required)")
    private String id;
    private Map<String, Object> configuration;

    public ConfigUpdateParams() {
    }

    public ConfigUpdateParams(String id, Map<String, Object> configuration) {
        this.id = id;
        this.configuration = configuration;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ConfigUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", configuration=").append(configuration);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ConfigUpdateParams setId(String id) {
        this.id = id;
        return this;
    }

    public Map<String, Object> getConfiguration() {
        return configuration;
    }

    public ConfigUpdateParams setConfiguration(Map<String, Object> configuration) {
        this.configuration = configuration;
        return this;
    }
}
