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

/**
 * Created by jacobo on 12/12/14.
 */
public class Annotation {

    /**
     * This id must be a valid variable ID.
     */
    private String id;
    private Object value;


    public Annotation() {
    }

    public Annotation(String id, Object value) {
        this.id = id;
        this.value = value;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Annotation{");
        sb.append("id='").append(id).append('\'');
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Annotation)) {
            return false;
        }

        Annotation that = (Annotation) o;

        if (!id.equals(that.id)) {
            return false;
        }
        if (!value.equals(that.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    public String getId() {
        return id;
    }

    public Annotation setId(String id) {
        this.id = id;
        return this;
    }

    public Object getValue() {
        return value;
    }

    public Annotation setValue(Object value) {
        this.value = value;
        return this;
    }
}
