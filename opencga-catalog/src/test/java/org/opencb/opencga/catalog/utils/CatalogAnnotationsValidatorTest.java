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
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.Variable;

import java.util.*;

/**
 * Created by jacobo on 23/06/15.
 */
public class CatalogAnnotationsValidatorTest {

    public static final Variable string = new Variable(
            "string", "", Variable.VariableType.TEXT, null, true, false, null, 0, null, null, null, null);
    public static final Variable stringNoRequired = new Variable(
            "string", "", Variable.VariableType.TEXT, null, false, false, null, 0, null, null, null, null);
    public static final Variable stringList = new Variable(
            "stringList", "", Variable.VariableType.TEXT, null, true, true, null, 0, null, null, null, null);
    public static final Variable numberList = new Variable(
            "numberList", "", Variable.VariableType.DOUBLE, null, true, true, null, 0, null, null, null, null);
    public static final Variable object = new Variable(
            "object", "", Variable.VariableType.OBJECT, null, true, false, null, 0, null, null,
            new HashSet<>(Arrays.<Variable>asList(string, numberList)), null);
    public static final Variable freeObject = new Variable(
            "freeObject", "", Variable.VariableType.OBJECT, null, true, false, null, 0, null, null, null, null);
    public static final Variable nestedObject = new Variable(
            "nestedObject", "", Variable.VariableType.OBJECT, null, true, false, null, 0, null, null,
            new HashSet<>(Arrays.<Variable>asList(stringList, object)), null);
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void checkVariableString() throws CatalogException {
        CatalogAnnotationsValidator.checkVariable(string);
    }

    @Test
    public void checkVariableStringList() throws CatalogException {
        CatalogAnnotationsValidator.checkVariable(stringList);
    }

    @Test
    public void checkVariableNumberList() throws CatalogException {
        CatalogAnnotationsValidator.checkVariable(numberList);
    }

    @Test
    public void checkVariableObject() throws CatalogException {
        CatalogAnnotationsValidator.checkVariable(object);
    }

    @Test
    public void checkStringTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(string.getName(), "value");
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(string.getName(), string), annotation);
    }

    @Test
    public void checkStringTest2() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(string.getName(), Arrays.asList("value1", "value2", "value3"));
        thrown.expect(CatalogException.class);
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(string.getName(), string), annotation);
    }

    @Test
    public void checkStringListTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(stringList.getName(), "value");
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(stringList.getName(), stringList), annotation);
    }

    @Test
    public void checkStringList2Test() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(stringList.getName(), Arrays.asList("value1", "value2", "value3"));
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(stringList.getName(), stringList), annotation);
    }

    @Test
    public void checkNumberListTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(numberList.getName(), Arrays.asList(1, "22", 3.3));
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(numberList.getName(), numberList), annotation);
    }

    @Test
    public void checkNumberListTest2() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(stringList.getName(), Arrays.asList(1, "NOT_A_NUMBER", 3.3));
        thrown.expect(CatalogException.class);
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(numberList.getName(), numberList), annotation);
    }

    @Test
    public void checkObjectTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(object.getName(), new ObjectMap(string.getName(), "OneString")
                .append(numberList.getName(), Arrays.asList(1, "2", 3)));
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(object.getName(), object), annotation);
    }

    @Test
    public void checkObjectTest2() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(object.getName(), new ObjectMap(string.getName(), "OneString")
                .append(numberList.getName(), Arrays.asList(1, 2, "K")));
        thrown.expect(CatalogException.class);
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(object.getName(), object), annotation);
    }

    @Test
    public void checkObjectTest3() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(object.getName(), new ObjectBean("string Key", Arrays.<Double>asList(3.3, 4.0, 5.0)));
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(object.getName(), object), annotation);
    }

    @Test
    public void checkFreeObjectTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(freeObject.getName(), new ObjectMap("ANY_KEY", "SOME_VALUE").append("A_BOOLEAN", false));
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(freeObject.getName(), freeObject), annotation);
    }

    @Test
    public void checkNestedObjectTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(nestedObject.getName(),
                new ObjectMap(stringList.getName(), Arrays.asList("v1", "v2", "v3"))
                        .append(object.getName(), new ObjectMap(string.getName(), "OneString")
                                .append(numberList.getName(), Arrays.asList (1, 2, "3")))
        );
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(nestedObject.getName(), nestedObject), annotation);
    }

    @Test
    public void checkNestedObjectTest2() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(nestedObject.getName(), new ObjectMap(stringList.getName(), Arrays.asList("v1", "v2", "v3"))
                .append(object.getName(), new ObjectMap(string.getName(), "OneString")
                        .append(numberList.getName(), Arrays.asList(1, "K", "3")))
        );
        thrown.expect(CatalogException.class);  //Bad value for numericList "K"
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(nestedObject.getName(), nestedObject), annotation);
    }

    @Test
    public void checkNestedObjectTest3() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(nestedObject.getName(), new ObjectMap(stringList.getName(), Arrays.asList("v1", "v2", "v3"))
                .append(object .getName(), new ObjectMap(string.getName(), "OneString")));
        thrown.expect(CatalogException.class); //Numeric list is required
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(nestedObject.getName(), nestedObject), annotation);
    }

    @Test
    public void checkNullValuesTest() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(string.getName(), null);
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(stringNoRequired.getName(), stringNoRequired), annotation);
    }

    @Test
    public void checkNullValues2Test() throws CatalogException {
        Map<String, Object> annotation = new HashMap<>();
        annotation.put(string.getName(), null);
        thrown.expect(CatalogException.class); //Numeric list is required
        CatalogAnnotationsValidator.checkAnnotations(Collections.singletonMap(string.getName(), string), annotation);
    }

    @Test
    public void mergeAnnotationsTest() {
        Map<String, Object> annotations = new HashMap<>();
        annotations.put("K", "V");
        annotations.put("K2", "V2");
        annotations.put("K4", false);
        AnnotationSet annotationSet = new AnnotationSet("", 0, annotations, "", 1, Collections.emptyMap());
        Map<String, Object> newAnnotations = new ObjectMap()
                .append("K", "newValue")        //Modify
                .append("K2", null)             //Delete
                .append("K3", "newAnnotation"); //Add
        CatalogAnnotationsValidator.mergeNewAnnotations(annotationSet, newAnnotations);

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