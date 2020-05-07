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

import org.opencb.opencga.core.common.TimeUtils;

public class CustomStatusParams {

    private String name;
    private String description;

    public CustomStatusParams() {
    }

    public CustomStatusParams(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public static CustomStatusParams of(CustomStatus status) {
        if (status != null) {
            return new CustomStatusParams(status.getName(), status.getDescription());
        } else {
            return null;
        }
    }

    public CustomStatus toCustomStatus() {
        return new CustomStatus(name, description, TimeUtils.getTime());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomStatusParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public CustomStatusParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public CustomStatusParams setDescription(String description) {
        this.description = description;
        return this;
    }
}
