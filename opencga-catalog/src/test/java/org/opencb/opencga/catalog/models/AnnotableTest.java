package org.opencb.opencga.catalog.models;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 26/07/16.
 */
public class AnnotableTest {

    @Test
    public void getAsMapTest() {
        Annotable annotable = new Sample();

        Set<Annotation> annotationList = new HashSet<>(8);
        annotationList.add(new Annotation("name1", "value1"));
        annotationList.add(new Annotation("name2", "value2"));
        annotationList.add(new Annotation("name3", Arrays.asList(1,2,3,4)));

        Set<Annotation> copy = new HashSet<>(6);
        for (Annotation annotation : annotationList) {
            copy.add(annotation);
        }

        annotationList.add(new Annotation("name4", copy));

        List<AnnotationSet> annotationSetList = new ArrayList<>(2);
        annotationSetList.add(new AnnotationSet("annot1", 1, annotationList, TimeUtils.getTime(), Collections.emptyMap()));
        annotationSetList.add(new AnnotationSet("annot2", 1, copy, TimeUtils.getTime(), Collections.emptyMap()));

        annotable.setAnnotationSets(annotationSetList);

        List<ObjectMap> annotationSetAsMap = annotable.getAnnotationSetAsMap();

        assertEquals(2, annotationSetAsMap.size());

        ObjectMap annotation1 = (ObjectMap) annotationSetAsMap.get(0).get("annotations");
        ObjectMap annotation2 = (ObjectMap) annotationSetAsMap.get(1).get("annotations");

        assertTrue(annotation1.containsKey("name1"));
        assertEquals("value1", annotation1.getString("name1"));

        // List data
        assertArrayEquals(Arrays.asList(1,2,3,4).toArray(), annotation1.getAsIntegerList("name3").toArray());

        // Nested data
        Object name4 = annotation1.get("name4");
        assertTrue(name4 instanceof ObjectMap);

        ObjectMap nestedData = (ObjectMap) name4;
        assertEquals("value1", nestedData.getString("name1"));
        assertArrayEquals(Arrays.asList(1,2,3,4).toArray(), nestedData.getAsIntegerList("name3").toArray());

        assertEquals(3, annotation2.size());
    }

}
