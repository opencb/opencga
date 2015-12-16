package org.opencb.opencga.catalog.utils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Annotation;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Variable;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jacobo on 23/06/15.
 */
public class CatalogAnnotationsValidatorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public static final Variable string = new Variable(
            "string", "", Variable.VariableType.TEXT, null, true, false, null, 0, null, null, null, null);

    public static final Variable stringNoRequired = new Variable(
            "string", "", Variable.VariableType.TEXT, null, false, false, null, 0, null, null, null, null);

    public static final Variable stringList = new Variable(
            "stringList", "", Variable.VariableType.TEXT, null, true, true, null, 0, null, null, null, null);

    public static final Variable numberList = new Variable(
            "numberList", "", Variable.VariableType.NUMERIC, null, true, true, null, 0, null, null, null, null);

    public static final Variable object = new Variable(
            "object", "", Variable.VariableType.OBJECT, null, true, false, null, 0, null, null,
            new HashSet<>(Arrays.<Variable>asList(string, numberList)), null);

    public static final Variable freeObject = new Variable(
            "freeObject", "", Variable.VariableType.OBJECT, null, true, false, null, 0, null, null, null, null);

    public static final Variable nestedObject = new Variable(
            "nestedObject", "", Variable.VariableType.OBJECT, null, true, false, null, 0, null, null,
            new HashSet<>(Arrays.<Variable>asList(stringList, object)), null);

    static public class ObjectBean {
        String string;
        List<Double> numberList;

        public ObjectBean() {}
        public ObjectBean(String string, List<Double> numberList) {this.string = string;this.numberList = numberList;}
        public String getString() {return string;}
        public void setString(String string) {this.string = string;}
        public List<Double> getNumberList() {return numberList;}
        public void setNumberList(List<Double> numberList) {this.numberList = numberList;}
    }

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
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(string.getId(), string), new Annotation(string.getId(), "value"));
    }

    @Test
    public void checkStringTest2() throws CatalogException {
        thrown.expect(CatalogException.class);
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(string.getId(), string), new Annotation(string.getId(), Arrays.asList("value1", "value2", "value3")));
    }

    @Test
    public void checkStringListTest() throws CatalogException {
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(stringList.getId(), stringList), new Annotation(stringList.getId(), "value"));
    }

    @Test
    public void checkStringList2Test() throws CatalogException {
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(stringList.getId(), stringList), new Annotation(stringList.getId(), Arrays.asList("value1", "value2", "value3")));
    }

    @Test
    public void checkNumberListTest() throws CatalogException {
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(numberList.getId(), numberList), new Annotation(numberList.getId(), Arrays.asList(1, "22", 3.3)));
    }

    @Test
    public void checkNumberListTest2() throws CatalogException {
        thrown.expect(CatalogException.class);
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(numberList.getId(), numberList), new Annotation(stringList.getId(), Arrays.asList(1, "NOT_A_NUMBER", 3.3)));
    }

    @Test
    public void checkObjectTest() throws CatalogException {
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(object.getId(), object),
                new Annotation(object.getId(), new ObjectMap(string.getId(), "OneString").append(numberList.getId(), Arrays.asList(1, "2", 3))));
    }

    @Test
    public void checkObjectTest2() throws CatalogException {
        thrown.expect(CatalogException.class);
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(object.getId(), object),
                new Annotation(object.getId(), new ObjectMap(string.getId(), "OneString").append(numberList.getId(), Arrays.asList(1,2,"K"))));
    }

    @Test
    public void checkObjectTest3() throws CatalogException {
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(object.getId(), object),
                new Annotation(object.getId(), new ObjectBean("string Key", Arrays.<Double>asList(3.3, 4.0, 5.0))));;
    }

    @Test
    public void checkFreeObjectTest() throws CatalogException {
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(freeObject.getId(), freeObject),
                new Annotation(freeObject.getId(), new ObjectMap("ANY_KEY", "SOME_VALUE").append("A_BOOLEAN", false)));
    }

    @Test
    public void checkNestedObjectTest() throws CatalogException {
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(nestedObject.getId(), nestedObject),
                new Annotation(nestedObject.getId(), new ObjectMap(stringList.getId(), Arrays.asList("v1", "v2", "v3")).append(object.getId(),
                        new ObjectMap(string.getId(), "OneString").append(numberList.getId(), Arrays.asList(1, 2, "3")))));
    }

    @Test
    public void checkNestedObjectTest2() throws CatalogException {
        thrown.expect(CatalogException.class);  //Bad value for numericList "K"
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(nestedObject.getId(), nestedObject),
                new Annotation(nestedObject.getId(), new ObjectMap(stringList.getId(), Arrays.asList("v1", "v2", "v3")).append(object.getId(),
                        new ObjectMap(string.getId(), "OneString").append(numberList.getId(), Arrays.asList(1, "K", "3")))));
    }

    @Test
    public void checkNestedObjectTest3() throws CatalogException {
        thrown.expect(CatalogException.class); //Numeric list is required
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(nestedObject.getId(), nestedObject),
                new Annotation(nestedObject.getId(), new ObjectMap(stringList.getId(), Arrays.asList("v1", "v2", "v3")).append(object.getId(),
                        new ObjectMap(string.getId(), "OneString"))));
    }

    @Test
    public void checkNullValuesTest() throws CatalogException {
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(stringNoRequired.getId(), stringNoRequired), new Annotation(string.getId(), null));
    }
    @Test
    public void checkNullValues2Test() throws CatalogException {
        thrown.expect(CatalogException.class); //Numeric list is required
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap(string.getId(), string), new Annotation(string.getId(), null));
    }

    @Test
    public void mergeAnnotationsTest() {
        HashSet<Annotation> annotations = new HashSet<>(Arrays.asList(
                new Annotation("K", "V"),
                new Annotation("K2", "V2"),
                new Annotation("K4", false)));
        AnnotationSet annotationSet = new AnnotationSet("", 0, annotations, "", Collections.emptyMap());
        Map<String, Object> newAnnotations = new ObjectMap()
                .append("K", "newValue")        //Modify
                .append("K2", null)             //Delete
                .append("K3", "newAnnotation"); //Add
        CatalogAnnotationsValidator.mergeNewAnnotations(annotationSet, newAnnotations);

        Map<String, Object> newAnnotation = annotationSet.getAnnotations().stream().collect(Collectors.toMap(Annotation::getId, Annotation::getValue));

        Assert.assertEquals(3, newAnnotation.size());
        Assert.assertEquals("newValue", newAnnotation.get("K"));
        Assert.assertEquals(false, newAnnotation.containsKey("K2"));
        Assert.assertEquals("newAnnotation", newAnnotation.get("K3"));
        Assert.assertEquals(false, newAnnotation.get("K4"));
    }

}