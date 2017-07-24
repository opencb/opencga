/*
 * Copyright 2015-2017 OpenCB
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

import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 11/09/14.
 */
public class Experiment {

    private long id;
    private String name;
    private String type;
    private String platform;
    private String manufacturer;
    private String date;
    private String lab;
    private String center;
    private String responsible;
    private String description;

    private Map<String, Object> attributes;


    public Experiment() {
    }

    public Experiment(int id, String name, String type, String platform, String manufacturer, String date, String lab,
                      String center, String responsible, String description) {
        this(id, name, type, platform, manufacturer, date, lab, center, responsible, description, new HashMap<>());
    }

    public Experiment(int id, String name, String type, String platform, String manufacturer, String date,  String lab,
                      String center, String responsible, String description, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.platform = platform;
        this.manufacturer = manufacturer;
        this.date = date;
        this.lab = lab;
        this.center = center;
        this.responsible = responsible;
        this.description = description;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Experiment{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", platform='").append(platform).append('\'');
        sb.append(", manufacturer='").append(manufacturer).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", lab='").append(lab).append('\'');
        sb.append(", center='").append(center).append('\'');
        sb.append(", responsible='").append(responsible).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public Experiment setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Experiment setName(String name) {
        this.name = name;
        return this;
    }

    public String getType() {
        return type;
    }

    public Experiment setType(String type) {
        this.type = type;
        return this;
    }

    public String getPlatform() {
        return platform;
    }

    public Experiment setPlatform(String platform) {
        this.platform = platform;
        return this;
    }

    public String getManufacturer() {
        return manufacturer;
    }

    public Experiment setManufacturer(String manufacturer) {
        this.manufacturer = manufacturer;
        return this;
    }

    public String getDate() {
        return date;
    }

    public Experiment setDate(String date) {
        this.date = date;
        return this;
    }

    public String getLab() {
        return lab;
    }

    public Experiment setLab(String lab) {
        this.lab = lab;
        return this;
    }

    public String getCenter() {
        return center;
    }

    public Experiment setCenter(String center) {
        this.center = center;
        return this;
    }

    public String getResponsible() {
        return responsible;
    }

    public Experiment setResponsible(String responsible) {
        this.responsible = responsible;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Experiment setDescription(String description) {
        this.description = description;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Experiment setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

}
