/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.catalog.models;

import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 17/12/14.
 */
public class Cohort {

    private int id;
    private String name;
    private String creationDate;
    private String description;

    private List<Integer> samples;

    private Map<String, Object> attributes;

    public Cohort() {
    }

    public Cohort(String name, String creationDate, String description, List<Integer> samples,
                  Map<String, Object> attributes) {
        this(-1, name, creationDate, description, samples, attributes);
    }

    public Cohort(int id, String name, String creationDate, String description, List<Integer> samples,
                  Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.creationDate = creationDate;
        this.description = description;
        this.samples = samples;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Cohort{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", creationDate='" + creationDate + '\'' +
                ", description='" + description + '\'' +
                ", samples=" + samples +
                ", attributes=" + attributes +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<Integer> getSamples() {
        return samples;
    }

    public void setSamples(List<Integer> samples) {
        this.samples = samples;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
