package org.opencb.opencga.app.cli.admin.executors.migration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.Annotation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class AnnotableForMigration {

    private long id;
    private List<AnnotationSet> annotationSets;

    public AnnotableForMigration() {
    }

    public long getId() {
        return id;
    }

    public AnnotableForMigration setId(long id) {
        this.id = id;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public AnnotableForMigration setAnnotationSets(List<AnnotationSet> annotationSets) {
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

    public class AnnotationSet {
        private String name;
        private long variableSetId;
        private List<Annotation> annotations;

        public AnnotationSet() {
        }

        public AnnotationSet(String name, long variableSetId, List<Annotation> annotations) {
            this.name = name;
            this.variableSetId = variableSetId;
            this.annotations = annotations;
        }

        public String getName() {
            return name;
        }

        public AnnotationSet setName(String name) {
            this.name = name;
            return this;
        }

        public long getVariableSetId() {
            return variableSetId;
        }

        public AnnotationSet setVariableSetId(long variableSetId) {
            this.variableSetId = variableSetId;
            return this;
        }

        public List<Annotation> getAnnotations() {
            return annotations;
        }

        public AnnotationSet setAnnotations(List<Annotation> annotations) {
            this.annotations = annotations;
            return this;
        }

    }
}
