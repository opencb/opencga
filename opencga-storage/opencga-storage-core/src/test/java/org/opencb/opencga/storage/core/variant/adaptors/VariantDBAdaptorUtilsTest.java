/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.core.variant.adaptors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.test.GenericTest;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils.isValidParam;

/**
 * Created on 01/02/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantDBAdaptorUtilsTest extends GenericTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testCheckOperatorAND() throws Exception {
        assertEquals(VariantDBAdaptorUtils.QueryOperation.AND, VariantDBAdaptorUtils.checkOperator("a;b;c"));
    }

    @Test
    public void testCheckOperatorOR() throws Exception {
        assertEquals(VariantDBAdaptorUtils.QueryOperation.OR, VariantDBAdaptorUtils.checkOperator("a,b,c"));
    }

    @Test
    public void testCheckOperatorANY() throws Exception {
        assertNull(VariantDBAdaptorUtils.checkOperator("a"));
    }

    @Test
    public void testCheckOperatorMix() throws Exception {
        thrown.expect(VariantQueryException.class);
        VariantDBAdaptorUtils.checkOperator("a,b;c");
    }

    @Test
    public void testSplitOperator() throws Exception {
        assertArrayEquals(new String[]{"key", "=", "value"}, VariantDBAdaptorUtils.splitOperator("key=value"));
    }

    @Test
    public void testSplitOperatorTrim() throws Exception {
        assertArrayEquals(new String[]{"key", "=", "value"}, VariantDBAdaptorUtils.splitOperator("key = value"));
    }

    @Test
    public void testSplitOperatorMissingKey() throws Exception {
        assertArrayEquals(new String[]{"", "<", "value"}, VariantDBAdaptorUtils.splitOperator("<value"));
    }

    @Test
    public void testSplitOperatorNoOperator() throws Exception {
        assertArrayEquals(new String[]{null, "=", "value"}, VariantDBAdaptorUtils.splitOperator("value"));
    }

    @Test
    public void testSplitOperatorUnknownOperator() throws Exception {
        assertArrayEquals(new String[]{null, "=", ">>>value"}, VariantDBAdaptorUtils.splitOperator(">>>value"));
    }

    @Test
    public void testSplitOperators() throws Exception {
        test("=");
        test("==");
        test("!");
        test("!=");
        test("<");
        test("<=");
        test(">");
        test(">=");
        test("~");
        test("=~");
    }

    private void test(String operator) {
        test("key", operator, "value");
        test("", operator, "value");
    }

    private void test(String key, String operator, String value) {
        assertArrayEquals("Split " + key + operator + value, new String[]{key, operator, value}, VariantDBAdaptorUtils.splitOperator(key + operator + value));
    }

    @Test
    public void testIsValid() {
        assertFalse(isValidParam(new Query(), ANNOTATION_EXISTS));
        assertFalse(isValidParam(new Query(ANNOTATION_EXISTS.key(), null), ANNOTATION_EXISTS));
        assertFalse(isValidParam(new Query(ANNOTATION_EXISTS.key(), ""), ANNOTATION_EXISTS));
        assertFalse(isValidParam(new Query(ANNOTATION_EXISTS.key(), Collections.emptyList()), ANNOTATION_EXISTS));
        assertFalse(isValidParam(new Query(ANNOTATION_EXISTS.key(), Arrays.asList()), ANNOTATION_EXISTS));

        assertTrue(isValidParam(new Query(ANNOTATION_EXISTS.key(), Arrays.asList(1,2,3)), ANNOTATION_EXISTS));
        assertTrue(isValidParam(new Query(ANNOTATION_EXISTS.key(), 5), ANNOTATION_EXISTS));
        assertTrue(isValidParam(new Query(ANNOTATION_EXISTS.key(), "sdfas"), ANNOTATION_EXISTS));
    }
}