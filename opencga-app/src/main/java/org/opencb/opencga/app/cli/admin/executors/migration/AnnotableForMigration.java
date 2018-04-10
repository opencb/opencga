package org.opencb.opencga.app.cli.admin.executors.migration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.Annotation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class AnnotableForMigration {

    private long id;
    private List<OldAnnotationSet> annotationSets;

    public AnnotableForMigration() {
    }

    public AnnotableForMigration(long id, List<OldAnnotationSet> annotationSets) {
        this.id = id;
        this.annotationSets = annotationSets;
    }

    public long getId() {
        return id;
    }

    public AnnotableForMigration setId(long id) {
        this.id = id;
        return this;
    }

    public List<OldAnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public AnnotableForMigration setAnnotationSets(List<OldAnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnnotableForMigration{");
        sb.append("id=").append(id);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append('}');
        return sb.toString();
    }

    @JsonIgnore
    public List<ObjectMap> getAnnotationSetAsMap() {
        List<ObjectMap> objectMapList = new ArrayList<>(annotationSets.size());

        for (OldAnnotationSet annotationSet : annotationSets) {
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

}
