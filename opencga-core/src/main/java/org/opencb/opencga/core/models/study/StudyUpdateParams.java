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

package org.opencb.opencga.core.models.study;

import org.opencb.opencga.core.models.common.CustomStatusParams;

import java.util.Map;

public class StudyUpdateParams {

    private String name;
    private String alias;
    private String description;
    private StudyNotification notification;

    private Map<String, Object> attributes;
    private CustomStatusParams status;

    public StudyUpdateParams() {
    }

    public StudyUpdateParams(String name, String alias, String description, StudyNotification notification, Map<String, Object> attributes,
                             CustomStatusParams status) {
        this.name = name;
        this.alias = alias;
        this.description = description;
        this.notification = notification;
        this.attributes = attributes;
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyUpdateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", alias='").append(alias).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", notification=").append(notification);
        sb.append(", attributes=").append(attributes);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public StudyUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public StudyUpdateParams setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public StudyUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public StudyNotification getNotification() {
        return notification;
    }

    public StudyUpdateParams setNotification(StudyNotification notification) {
        this.notification = notification;
        return this;
    }

    public CustomStatusParams getStatus() {
        return status;
    }

    public StudyUpdateParams setStatus(CustomStatusParams status) {
        this.status = status;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public StudyUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
