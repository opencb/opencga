package org.opencb.opencga.catalog.utils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class CheckUtilsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void compareStringCheckTest() throws CatalogException {
        assertTrue(CheckUtils.check("COMPARE_STRING(hello, hello, ==)"));
        assertTrue(CheckUtils.check("COMPARE_STRING(hello, hello, =)"));
        assertFalse(CheckUtils.check("COMPARE_STRING(hello, hello, !=)"));
        assertTrue(CheckUtils.check("COMPARE_STRING(hello, helloo, !=)"));
        assertFalse(CheckUtils.check("COMPARE_STRING(hello, helloo, ==)"));
    }

    @Test
    public void compareBooleanCheckTest() throws CatalogException {
        assertTrue(CheckUtils.check("COMPARE_BOOLEAN(true, True, ==)"));
        assertTrue(CheckUtils.check("COMPARE_BOOLEAN(true, True, =)"));
        assertTrue(CheckUtils.check("COMPARE_BOOLEAN(true, False, !=)"));
        assertFalse(CheckUtils.check("COMPARE_BOOLEAN(false, False, !=)"));
        assertTrue(CheckUtils.check("COMPARE_BOOLEAN(false, False, ==)"));

        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage("Cannot cast 'hello'");
        CheckUtils.check("COMPARE_BOOLEAN(true, hello, ==)");
    }

    @Test
    public void compareNumberCheckTest() throws CatalogException {
        assertTrue(CheckUtils.check("COMPARE_NUMBER(5, 5.0, ==)"));
        assertTrue(CheckUtils.check("COMPARE_NUMBER(5, 5.0, =)"));
        assertTrue(CheckUtils.check("COMPARE_NUMBER(5, 5.0, <=)"));
        assertTrue(CheckUtils.check("COMPARE_NUMBER(5, 5.0, >=)"));
        assertFalse(CheckUtils.check("COMPARE_NUMBER(5, 5.0, <)"));
        assertFalse(CheckUtils.check("COMPARE_NUMBER(5, 5.0, >)"));
        assertFalse(CheckUtils.check("COMPARE_NUMBER(5, 5.0, !=)"));
        assertTrue(CheckUtils.check("COMPARE_NUMBER(5, 6, <)"));
        assertFalse(CheckUtils.check("COMPARE_NUMBER(5, 6, >)"));
        assertFalse(CheckUtils.check("COMPARE_NUMBER(6, 5, <)"));
        assertTrue(CheckUtils.check("COMPARE_NUMBER(6, 5, >)"));

        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage("Cannot cast 'bla'");
        CheckUtils.check("COMPARE_NUMBER(6, bla, >)");
    }

    @Test
    public void compareListCheckTest() throws CatalogException {
        List<String> list = Arrays.asList("a", "b", "c");
        assertTrue(CheckUtils.check("COMPARE_LIST_SIZE(" + list + ", 3, =)"));
        assertTrue(CheckUtils.check("COMPARE_LIST_SIZE(" + list + ", 3, >=)"));
        assertTrue(CheckUtils.check("COMPARE_LIST_SIZE(" + list + ", 3, <=)"));
        assertTrue(CheckUtils.check("COMPARE_LIST_SIZE(" + list + ", 2, >)"));
        assertTrue(CheckUtils.check("COMPARE_LIST_SIZE(" + list + ", 4, <)"));
        assertFalse(CheckUtils.check("COMPARE_LIST_SIZE(" + list + ", 4, >)"));
        assertFalse(CheckUtils.check("COMPARE_LIST_SIZE(" + list + ", 2, <)"));

        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage("Cannot cast '6'");
        CheckUtils.check("COMPARE_LIST_SIZE(6, 5, >)");
    }

    @Test
    public void isEmptyObjectCheckTest() throws CatalogException {
        Map<String, String> map = new HashMap<>();
        map.put("a", "b");

        assertFalse(CheckUtils.check("IS_EMPTY_OBJECT(" + map + ", true)"));
        assertTrue(CheckUtils.check("IS_EMPTY_OBJECT(" + map + ", false)"));

        map = new HashMap<>();
        assertTrue(CheckUtils.check("IS_EMPTY_OBJECT(" + map + ", true)"));
        assertFalse(CheckUtils.check("IS_EMPTY_OBJECT(" + map + ", false)"));

        try {
            CheckUtils.check("IS_EMPTY_OBJECT(" + map + ", blo)");
            fail("blo cannot be parsed to true|false");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("Unknown operator 'blo'"));
        }

        try {
            CheckUtils.check("IS_EMPTY_OBJECT([1,2,3], true)");
            fail("[1,2,3] cannot be parsed to map");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("Cannot cast '[1,2,3]'"));
        }
    }

    @Test
    public void existsCheckTest() throws CatalogException {
        Map<String, String> map = new HashMap<>();
        map.put("a", "b");

        assertTrue(CheckUtils.check("EXISTS(" + map + ", true)"));
        assertFalse(CheckUtils.check("EXISTS(" + map + ", false)"));

        assertFalse(CheckUtils.check("EXISTS(" + null + ", true)"));
        assertTrue(CheckUtils.check("EXISTS(" + null + ", false)"));
        assertFalse(CheckUtils.check("EXISTS(, true)"));
        assertTrue(CheckUtils.check("EXISTS(, false)"));
    }

}