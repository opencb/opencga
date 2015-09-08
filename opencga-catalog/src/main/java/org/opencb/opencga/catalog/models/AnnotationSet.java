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

import java.util.Map;
import java.util.Set;

/**
 * Created by imedina on 25/11/14.
 */
public class AnnotationSet {

    private String id;
    private int variableSetId;
    private Set<Annotation> annotations;
    private String date;

    private Map<String, Object> attributes;


    public AnnotationSet() {
    }

    public AnnotationSet(String id, int variableSetId, Set<Annotation> annotations, String date,
                         Map<String, Object> attributes) {
        this.id = id;
        this.variableSetId = variableSetId;
        this.annotations = annotations;
        this.date = date;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "AnnotationSet{" +
                "id='" + id + '\'' +
                ", variableSetId=" + variableSetId +
                ", annotations=" + annotations +
                ", date='" + date + '\'' +
                ", attributes=" + attributes +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AnnotationSet)) return false;

        AnnotationSet that = (AnnotationSet) o;

        if (variableSetId != that.variableSetId) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (annotations != null ? !annotations.equals(that.annotations) : that.annotations != null) return false;
        if (date != null ? !date.equals(that.date) : that.date != null) return false;
        return !(attributes != null ? !attributes.equals(that.attributes) : that.attributes != null);

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + variableSetId;
        result = 31 * result + (annotations != null ? annotations.hashCode() : 0);
        result = 31 * result + (date != null ? date.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getVariableSetId() {
        return variableSetId;
    }

    public void setVariableSetId(int variableSetId) {
        this.variableSetId = variableSetId;
    }

    public Set<Annotation> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(Set<Annotation> annotations) {
        this.annotations = annotations;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
