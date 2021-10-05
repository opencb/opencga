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

package org.opencb.opencga.core.models.sample;

import java.util.Map;

public class SampleCollection {

    private String tissue;
    private String organ;
    private String quantity;
    private String method;
    private String date;
    private Map<String, Object> attributes;

    public SampleCollection() {
    }

    public SampleCollection(String tissue, String organ, String quantity, String method, String date, Map<String, Object> attributes) {
        this.tissue = tissue;
        this.organ = organ;
        this.quantity = quantity;
        this.method = method;
        this.date = date;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleCollection{");
        sb.append("tissue='").append(tissue).append('\'');
        sb.append(", organ='").append(organ).append('\'');
        sb.append(", quantity='").append(quantity).append('\'');
        sb.append(", method='").append(method).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", attributes='").append(attributes).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getTissue() {
        return tissue;
    }

    public SampleCollection setTissue(String tissue) {
        this.tissue = tissue;
        return this;
    }

    public String getOrgan() {
        return organ;
    }

    public SampleCollection setOrgan(String organ) {
        this.organ = organ;
        return this;
    }

    public String getQuantity() {
        return quantity;
    }

    public SampleCollection setQuantity(String quantity) {
        this.quantity = quantity;
        return this;
    }

    public String getMethod() {
        return method;
    }

    public SampleCollection setMethod(String method) {
        this.method = method;
        return this;
    }

    public String getDate() {
        return date;
    }

    public SampleCollection setDate(String date) {
        this.date = date;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public SampleCollection setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
