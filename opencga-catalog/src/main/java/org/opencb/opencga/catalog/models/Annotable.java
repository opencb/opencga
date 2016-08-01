package org.opencb.opencga.catalog.models;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by pfurio on 07/07/16.
 */
public abstract class Annotable {

    protected List<AnnotationSet> annotationSets;

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public Annotable setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

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
