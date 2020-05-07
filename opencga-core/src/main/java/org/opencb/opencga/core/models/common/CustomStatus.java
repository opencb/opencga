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

public class CustomStatus {

    private String name;
    private String description;
    private String date;

    public CustomStatus() {
        this("", "", "");
    }

    public CustomStatus(String name, String description, String date) {
        this.name = name;
        this.description = description;
        this.date = date;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomStatus{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public CustomStatus setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public CustomStatus setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getDate() {
        return date;
    }

    public CustomStatus setDate(String date) {
        this.date = date;
        return this;
    }
}
