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

package org.opencb.opencga.core.models.common;

import org.opencb.biodata.models.common.Status;
import org.opencb.opencga.core.common.TimeUtils;

public class StatusParams {

    private String id;
    private String name;
    private String description;

    public StatusParams() {
    }

    public StatusParams(String id, String name, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
    }

    public static StatusParams of(Status status) {
        if (status != null) {
            return new StatusParams(status.getId(), status.getName(), status.getDescription());
        } else {
            return null;
        }
    }

    public Status toStatus() {
        return new Status(id, name, description, TimeUtils.getTime());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StatusParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public StatusParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public StatusParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public StatusParams setDescription(String description) {
        this.description = description;
        return this;
    }
}
