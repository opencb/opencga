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

package org.opencb.opencga.core.models.clinical.pipeline;

import org.opencb.commons.annotations.DataField;

import java.util.Map;

public class PipelineStep {

    @DataField(id = "active", description = "Active step")
    protected Boolean active;

    @DataField(id = "options", description = "Step-specific options")
    protected Map<String, Object> options;

    public PipelineStep() {
    }

    public PipelineStep(Map<String, Object> options) {
        this.options = options;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public PipelineStep(Boolean active, Map<String, Object> options) {
        this.active = active;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineStep{");
        sb.append("active=").append(active);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public Boolean getActive() {
        return active;
    }

    public PipelineStep setActive(Boolean active) {
        this.active = active;
        return this;
    }

    public PipelineStep setOptions(Map<String, Object> options) {
        this.options = options;
        return this;
    }
}