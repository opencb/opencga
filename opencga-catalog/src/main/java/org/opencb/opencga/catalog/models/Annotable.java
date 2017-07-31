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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.models.acls.AbstractAcl;
import org.opencb.opencga.catalog.models.acls.permissions.AbstractAclEntry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by pfurio on 07/07/16.
 */
public abstract class Annotable<T extends AbstractAclEntry> extends AbstractAcl<T> {

    protected List<AnnotationSet> annotationSets;

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public Annotable setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    @JsonIgnore
    public List<ObjectMap> getAnnotationSetAsMap() {
        List<ObjectMap> objectMapList = new ArrayList<>(annotationSets.size());

        for (AnnotationSet annotationSet : annotationSets) {
            ObjectMap objectMap = new ObjectMap(10);

            objectMap.put("name", annotationSet.getName());
            objectMap.put("variableSetId", annotationSet.getVariableSetId());
            objectMap.put("creationDate", annotationSet.getCreationDate());
            objectMap.put("attributes", annotationSet.getAttributes());

            ObjectMap annotations = new ObjectMap(annotationSet.getAnnotations().size() * 2);
            for (Annotation annotation : annotationSet.getAnnotations()) {
                annotations.put(annotation.getName(), recursiveMap(annotation.getValue()));
            }
            objectMap.put("annotations", annotations);

            objectMapList.add(objectMap);
        }

        return objectMapList;
    }

    /**
     * Given a value from an annotation, it returns the result as a map. If the value corresponds with a nested map, it will return an
     * ObjectMap with the key: values as demanded. If the value given is not a map, it will be returned directly.
     *
     * @param value corresponds to the value object part from the annotation to be parsed.
     * @return the annotations as proper key:value pairs.
     */
    private Object recursiveMap(Object value) {
        if (value instanceof HashSet) {
            HashSet<Annotation> valueMap = (HashSet<Annotation>) value;
            ObjectMap resultMap = new ObjectMap(valueMap.size() * 2);
            for (Annotation annotation : valueMap) {
                resultMap.put(annotation.getName(), recursiveMap(annotation.getValue()));
            }
            return resultMap;
        } else {
            return value;
        }
    }
}
