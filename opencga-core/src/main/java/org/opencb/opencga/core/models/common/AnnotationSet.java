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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.opencb.opencga.core.common.JacksonUtils;

import java.util.Map;

/**
 * Created by imedina on 25/11/14.
 */
public class AnnotationSet {

    private String id;
    private String variableSetId;
    private Map<String, Object> annotations;

    public AnnotationSet() {
    }

    public AnnotationSet(String id, String variableSetId, Map<String, Object> annotations) {
        this.id = id;
        this.variableSetId = variableSetId;
        this.annotations = annotations;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnnotationSet{");
        sb.append("id='").append(id).append('\'');
        sb.append(", variableSetId=").append(variableSetId);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AnnotationSet that = (AnnotationSet) o;
        return new EqualsBuilder().append(variableSetId, that.variableSetId).append(id, that.id).append(annotations, that.annotations)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(id).append(variableSetId).append(annotations).toHashCode();
    }

    public String getId() {
        return id;
    }

    public AnnotationSet setId(String id) {
        this.id = id;
        return this;
    }

    public String getVariableSetId() {
        return variableSetId;
    }

    public AnnotationSet setVariableSetId(String variableSetId) {
        this.variableSetId = variableSetId;
        return this;
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public AnnotationSet setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
        return this;
    }

    public <T> T to(Class<T> tClass) {
        return JacksonUtils.getDefaultObjectMapper().convertValue(this.getAnnotations(), tClass);
    }
}
