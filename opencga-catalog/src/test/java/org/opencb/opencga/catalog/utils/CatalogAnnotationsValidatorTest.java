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

package org.opencb.opencga.catalog.utils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.study.Variable;

import java.util.*;

/**
 * Created by jacobo on 23/06/15.
 */
public class CatalogAnnotationsValidatorTest {

    public static final Variable string = new Variable(
            "string", "", Variable.VariableType.STRING, null, true, false, null, null, 0, null, null, null, null);
    public static final Variable stringNoRequired = new Variable(
            "string", "", Variable.VariableType.STRING, null, false, false, null, null, 0, null, null, null, null);
    public static final Variable stringList = new Variable(
            "stringList", "", Variable.VariableType.STRING, null, true, true, null, null, 0, null, null, null, null);
    public static final Variable numberList = new Variable(
            "numberList", "", Variable.VariableType.DOUBLE, null, true, true, null, null, 0, null, null, null, null);
    public static final Variable object = new Variable(
            "object", "", Variable.VariableType.OBJECT, null, true, false, null, null, 0, null, null,
            new HashSet<>(Arrays.<Variable>asList(string, numberList)), null);
    public static final Variable freeObject = new Variable(
            "freeObject", "", Variable.VariableType.OBJECT, null, true, false, null, null, 0, null, null, null, null);
    public static final Variable nestedObject = new Variable(
            "nestedObject", "", Variable.VariableType.OBJECT, null, true, false, null, null, 0, null, null,
            new HashSet<>(Arrays.<Variable>asList(stringList, object)), null);
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void checkVariableString() throws CatalogException {
        AnnotationUtils.checkVariable(string);
    }

    @Test
    public void checkVariableStringList() throws CatalogException {
        AnnotationUtils.checkVariable(stringList);
    }

    @Test
    public void checkVariableNumberList() throws CatalogException {
        AnnotationUtils.checkVariable(numberList);
    }

    @Test
    public void checkVariableObject() throws CatalogException {
        AnnotationUtils.checkVariable(object);
    }

    @Test
    public void checkStringTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(string.getId(), "value");
        AnnotationUtils.checkAnnotations(Collections.singletonMap(string.getId(), string), annotation);
    }

    @Test
    public void checkStringTest2() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(string.getId(), Arrays.asList("value1", "value2", "value3"));
        thrown.expect(CatalogException.class);
        AnnotationUtils.checkAnnotations(Collections.singletonMap(string.getId(), string), annotation);
    }

    @Test
    public void checkStringListTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(stringList.getId(), "value");
        AnnotationUtils.checkAnnotations(Collections.singletonMap(stringList.getId(), stringList), annotation);
    }

    @Test
    public void checkStringList2Test() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(stringList.getId(), Arrays.asList("value1", "value2", "value3"));
        AnnotationUtils.checkAnnotations(Collections.singletonMap(stringList.getId(), stringList), annotation);
    }

    @Test
    public void checkNumberListTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(numberList.getId(), Arrays.asList(1, "22", 3.3));
        AnnotationUtils.checkAnnotations(Collections.singletonMap(numberList.getId(), numberList), annotation);
    }

    @Test
    public void checkNumberListTest2() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(stringList.getId(), Arrays.asList(1, "NOT_A_NUMBER", 3.3));
        thrown.expect(CatalogException.class);
        AnnotationUtils.checkAnnotations(Collections.singletonMap(numberList.getId(), numberList), annotation);
    }

    @Test
    public void checkObjectTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(object.getId(), new ObjectMap(string.getId(), "OneString")
                .append(numberList.getId(), Arrays.asList(1, "2", 3)));
        AnnotationUtils.checkAnnotations(Collections.singletonMap(object.getId(), object), annotation);
    }

    @Test
    public void checkObjectTest2() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(object.getId(), new ObjectMap(string.getId(), "OneString")
                .append(numberList.getId(), Arrays.asList(1, 2, "K")));
        thrown.expect(CatalogException.class);
        AnnotationUtils.checkAnnotations(Collections.singletonMap(object.getId(), object), annotation);
    }

    @Test
    public void checkObjectTest3() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(object.getId(), new ObjectBean("string Key", Arrays.<Double>asList(3.3, 4.0, 5.0)));
        AnnotationUtils.checkAnnotations(Collections.singletonMap(object.getId(), object), annotation);
    }

    @Test
    public void checkFreeObjectTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(freeObject.getId(), new ObjectMap("ANY_KEY", "SOME_VALUE").append("A_BOOLEAN", false));
        AnnotationUtils.checkAnnotations(Collections.singletonMap(freeObject.getId(), freeObject), annotation);
    }

    @Test
    public void checkNestedObjectTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(nestedObject.getId(),
                new ObjectMap(stringList.getId(), Arrays.asList("v1", "v2", "v3"))
                        .append(object.getId(), new ObjectMap(string.getId(), "OneString")
                                .append(numberList.getId(), Arrays.asList (1, 2, "3")))
        );
        AnnotationUtils.checkAnnotations(Collections.singletonMap(nestedObject.getId(), nestedObject), annotation);
    }

    @Test
    public void checkNestedObjectTest2() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(nestedObject.getId(), new ObjectMap(stringList.getId(), Arrays.asList("v1", "v2", "v3"))
                .append(object.getId(), new ObjectMap(string.getId(), "OneString")
                        .append(numberList.getId(), Arrays.asList(1, "K", "3")))
        );
        thrown.expect(CatalogException.class);  //Bad value for numericList "K"
        AnnotationUtils.checkAnnotations(Collections.singletonMap(nestedObject.getId(), nestedObject), annotation);
    }

    @Test
    public void checkNestedObjectTest3() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(nestedObject.getId(), new ObjectMap(stringList.getId(), Arrays.asList("v1", "v2", "v3"))
                .append(object .getId(), new ObjectMap(string.getId(), "OneString")));
        thrown.expect(CatalogException.class); //Numeric list is required
        AnnotationUtils.checkAnnotations(Collections.singletonMap(nestedObject.getId(), nestedObject), annotation);
    }

    @Test
    public void checkNullValuesTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(string.getId(), null);
        AnnotationUtils.checkAnnotations(Collections.singletonMap(stringNoRequired.getId(), stringNoRequired), annotation);
    }

    @Test
    public void checkNullValues2Test() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(string.getId(), null);
        thrown.expect(CatalogException.class); //Numeric list is required
        AnnotationUtils.checkAnnotations(Collections.singletonMap(string.getId(), string), annotation);
    }

    @Test
    public void mergeAnnotationsTest() {
        Map<String, Object> annotations = new HashMap<>();
        annotations.put("K", "V");
        annotations.put("K2", "V2");
        annotations.put("K4", false);
        AnnotationSet annotationSet = new AnnotationSet("", "", annotations, "", 1, Collections.emptyMap());
        Map<String, Object> newAnnotations = new ObjectMap()
                .append("K", "newValue")        //Modify
                .append("K2", null)             //Delete
                .append("K3", "newAnnotation"); //Add
        AnnotationUtils.mergeNewAnnotations(annotationSet, newAnnotations);

        Map<String, Object> newAnnotation = annotationSet.getAnnotations();

        Assert.assertEquals(4, newAnnotation.size());
        Assert.assertEquals("newValue", newAnnotation.get("K"));
        Assert.assertEquals(null, newAnnotation.get("K2"));
        Assert.assertEquals("newAnnotation", newAnnotation.get("K3"));
        Assert.assertEquals(false, newAnnotation.get("K4"));
    }

    static public class ObjectBean {
        String string;
        List<Double> numberList;

        public ObjectBean() {
        }

        public ObjectBean(String string, List<Double> numberList) {
            this.string = string;
            this.numberList = numberList;
        }

        public String getString() {
            return string;
        }

        public void setString(String string) {
            this.string = string;
        }

        public List<Double> getNumberList() {
            return numberList;
        }

        public void setNumberList(List<Double> numberList) {
            this.numberList = numberList;
        }
    }

}