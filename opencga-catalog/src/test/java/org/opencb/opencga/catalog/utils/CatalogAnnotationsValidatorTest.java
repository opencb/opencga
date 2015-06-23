package org.opencb.opencga.catalog.utils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Annotation;
import org.opencb.opencga.catalog.models.Variable;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Created by jacobo on 23/06/15.
 */
public class CatalogAnnotationsValidatorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Variable string = new Variable(
            "id", "", Variable.VariableType.TEXT, null, true, false, null, 0, null, null, null);

    private Variable stringList = new Variable(
            "id", "", Variable.VariableType.TEXT, null, true, true, null, 0, null, null, null);

    private Variable numberList = new Variable(
            "id", "", Variable.VariableType.NUMERIC, null, true, true, null, 0, null, null, null);

    @Test
    public void checkStringTest() throws CatalogException {
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap("id", string), new Annotation("id", "value"));
    }

    @Test
    public void checkStringTest2() throws CatalogException {
        thrown.expect(CatalogException.class);
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap("id", string), new Annotation("id", Arrays.asList("value1", "value2", "value3")));
    }

    @Test
    public void checkStringListTest() throws CatalogException {
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap("id", stringList), new Annotation("id", "value"));
    }

    @Test
    public void checkStringList2Test() throws CatalogException {
        CatalogAnnotationsValidator.checkAnnotation(Collections.singletonMap("id", stringList), new Annotation("id", Arrays.asList("value1", "value2", "value3")));
    }

}