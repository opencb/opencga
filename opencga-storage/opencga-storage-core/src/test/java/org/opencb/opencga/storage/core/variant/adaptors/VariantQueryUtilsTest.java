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

package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.lang.reflect.Field;
import java.util.*;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;

/**
 * Created on 01/02/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantQueryUtilsTest extends GenericTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testCheckOperatorAND() throws Exception {
        assertEquals(VariantQueryUtils.QueryOperation.AND, VariantQueryUtils.checkOperator("a;b;c"));
    }

    @Test
    public void testCheckOperatorOR() throws Exception {
        assertEquals(VariantQueryUtils.QueryOperation.OR, VariantQueryUtils.checkOperator("a,b,c"));
    }

    @Test
    public void testCheckOperatorANY() throws Exception {
        assertNull(VariantQueryUtils.checkOperator("a"));
    }

    @Test
    public void testCheckOperatorMix() throws Exception {
        thrown.expect(VariantQueryException.class);
        VariantQueryUtils.checkOperator("a,b;c");
    }

    @Test
    public void testCheckOperatorMixQuotesOr() throws Exception {
        assertEquals(VariantQueryUtils.QueryOperation.OR, VariantQueryUtils.checkOperator("a,\"b;c\""));
    }

    @Test
    public void testCheckOperatorMixQuotesAnd() throws Exception {
        assertEquals(VariantQueryUtils.QueryOperation.AND, VariantQueryUtils.checkOperator("a;\"b,c\""));
    }

    @Test
    public void testSplitQuotes() throws Exception {
        assertEquals(Arrays.asList("a", "b,c", "d"), VariantQueryUtils.splitQuotes("a;\"b,c\";d", ';'));
        assertEquals(Arrays.asList("a", "b;c", "d"), VariantQueryUtils.splitQuotes("a,\"b;c\",d", ','));
    }

    @Test
    public void testSplitQuotesUnbalanced() throws Exception {
        thrown.expect(VariantQueryException.class);
        VariantQueryUtils.splitQuotes("a,\"b;c\",\"d", ',');
    }

    @Test
    public void testSplitOperator() throws Exception {
        assertArrayEquals(new String[]{"key", "=", "value"}, VariantQueryUtils.splitOperator("key=value"));
    }

    @Test
    public void testSplitOperatorTrim() throws Exception {
        assertArrayEquals(new String[]{"key", "=", "value"}, VariantQueryUtils.splitOperator("key = value"));
    }

    @Test
    public void testSplitOperatorMissingKey() throws Exception {
        assertArrayEquals(new String[]{"", "<", "value"}, VariantQueryUtils.splitOperator("<value"));
    }

    @Test
    public void testSplitOperatorNoOperator() throws Exception {
        assertArrayEquals(new String[]{null, "=", "value"}, VariantQueryUtils.splitOperator("value"));
    }

    @Test
    public void testSplitOperatorUnknownOperator() throws Exception {
        assertArrayEquals(new String[]{null, "=", ">>>value"}, VariantQueryUtils.splitOperator(">>>value"));
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
        assertArrayEquals("Split " + key + operator + value, new String[]{key, operator, value}, VariantQueryUtils.splitOperator(key + operator + value));
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

    @Test
    public void testParseSO() throws Exception {
        assertEquals(1587, parseConsequenceType("stop_gained"));
        assertEquals(1587, parseConsequenceType("1587"));
        assertEquals(1587, parseConsequenceType("SO:00001587"));
    }

    @Test
    public void testParseWrongSOTerm() throws Exception {
        thrown.expect(VariantQueryException.class);
        parseConsequenceType("wrong_so");
    }

    @Test
    public void testParseWrongSONumber() throws Exception {
        thrown.expect(VariantQueryException.class);
        parseConsequenceType("9999999");
    }

    @Test
    public void testParseWrongSONumber2() throws Exception {
        thrown.expect(VariantQueryException.class);
        parseConsequenceType("SO:9999999");
    }

    @Test
    public void testParseGenotypeFilter() throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, List<String>> expected = new HashMap(new ObjectMap()
                .append("study:sample", Arrays.asList("1/1", "2/2"))
                .append("sample2", Arrays.asList("0/0", "2/2"))
                .append("sample3", Arrays.asList("0/0"))
                .append("study1:sample4", Arrays.asList("0/0", "2/2")));

        HashMap<Object, List<String>> map = new HashMap<>();
        assertEquals(VariantQueryUtils.QueryOperation.AND, parseGenotypeFilter("study:sample:1/1,2/2;sample2:0/0,2/2;sample3:0/0;study1:sample4:0/0,2/2", map));
        assertEquals(expected, map);

        map.clear();
        // Check ends with operator
        assertEquals(VariantQueryUtils.QueryOperation.AND, parseGenotypeFilter("study:sample:1/1,2/2;sample2:0/0,2/2;sample3:0/0;study1:sample4:0/0,2/2;", map));
        assertEquals(expected, map);

        map.clear();
        assertEquals(VariantQueryUtils.QueryOperation.OR, parseGenotypeFilter("study:sample:1/1,2/2,sample2:0/0,2/2,sample3:0/0,study1:sample4:0/0,2/2", map));
        assertEquals(expected, map);

        expected.put("sample3", Arrays.asList("!0/0"));
        map.clear();
        assertEquals(VariantQueryUtils.QueryOperation.OR, parseGenotypeFilter("study:sample:1/1,2/2,sample2:0/0,2/2,sample3:!0/0,study1:sample4:0/0,2/2", map));
        assertEquals(expected, map);
    }

    @Test
    public void testParseGenotypeFilterFail() throws Exception {
        HashMap<Object, List<String>> map = new HashMap<>();
        thrown.expect(VariantQueryException.class);
        parseGenotypeFilter("sample:1/1,2/2,sample2:0/0,2/2;sample3:0/0,2/2", map);
    }

    @Test
    public void testParseFormatFilter() throws Exception {

        checkParseFormat(new Query(SAMPLE.key(), "HG00096").append(SAMPLE_DATA.key(), "DP>8"),
                null,
                "HG00096", "DP>8");
        checkParseFormat(new Query(SAMPLE.key(), "HG00096,!HG00097").append(SAMPLE_DATA.key(), "DP>8"),
                null,
                "HG00096", "DP>8");
        checkParseFormat(new Query(SAMPLE.key(), "HG00096,HG00097").append(SAMPLE_DATA.key(), "DP>8"),
                QueryOperation.OR,
                "HG00096", "DP>8",
                "HG00097", "DP>8");
        checkParseFormat(new Query(SAMPLE.key(), "HG00096;HG00097").append(SAMPLE_DATA.key(), "DP>8"),
                QueryOperation.AND,
                "HG00096", "DP>8",
                "HG00097", "DP>8");

        checkParseFormat(new Query(GENOTYPE.key(), "HG00096:0/1").append(SAMPLE_DATA.key(), "DP>8"),
                null,
                "HG00096", "DP>8");
        checkParseFormat(new Query(GENOTYPE.key(), "HG00096:0/1,HG00097:0/1").append(SAMPLE_DATA.key(), "DP>8"),
                QueryOperation.OR,
                "HG00096", "DP>8",
                "HG00097", "DP>8");
        checkParseFormat(new Query(GENOTYPE.key(), "HG00096:0/1;HG00097:0/1").append(SAMPLE_DATA.key(), "DP>8"),
                QueryOperation.AND,
                "HG00096", "DP>8",
                "HG00097", "DP>8");

        checkParseFormat(new Query(SAMPLE_DATA.key(), "HG00096:GQ>5.0,HG00097:DP>8"),
                QueryOperation.OR,
                "HG00096", "GQ>5.0",
                "HG00097", "DP>8");
        checkParseFormat(new Query(SAMPLE_DATA.key(), "HG00097:DP>8,HG00096:GQ>5.0"),
                QueryOperation.OR,
                "HG00097", "DP>8",
                "HG00096", "GQ>5.0");
        checkParseFormat(new Query(SAMPLE_DATA.key(), "HG00096:GT=0/1,1/1;HG00097:GT=1/1;DP>3"),
                QueryOperation.AND,
                "HG00096", "GT=0/1,1/1",
                "HG00097", "GT=1/1;DP>3");
        checkParseFormat(new Query(SAMPLE_DATA.key(), "HG00096:GT=0/1,1/1;HG00097:GT=1/1;HG00097:DP>3"),
                QueryOperation.AND,
                "HG00096", "GT=0/1,1/1",
                "HG00097", "GT=1/1;DP>3");

        checkParseFormat(new Query(SAMPLE_DATA.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz:HaplotypeScore<10,"
                        + "1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz:DP>100"),
                QueryOperation.OR,
                "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz", "HaplotypeScore<10",
                "1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz", "DP>100");

    }

    protected void checkParseFormat(Query query, QueryOperation expectedOperation, String ...expected) {
        Pair<QueryOperation, Map<String, String>> pair = parseFormat(query);
        QueryOperation operation = pair.getKey();
        Map<String, String> map = pair.getValue();

        HashMap<String, String> expectedMap = new HashMap<>();

        for (int i = 0; i < expected.length; i+=2) {
            expectedMap.put(expected[i], expected[i + 1]);
        }

        assertEquals(expectedMap, map);
        assertEquals(expectedOperation, operation);

    }

    @Test
    public void extractGenotypeFromFormatTest() {
        assertEquals(new Query(SAMPLE_DATA.key(), "").append(GENOTYPE.key(), "S1:1/1"),
                extractGenotypeFromFormatFilter(new Query(SAMPLE_DATA.key(), "S1:GT=1/1")));
        assertEquals(new Query(SAMPLE_DATA.key(), "S1:DP>4").append(GENOTYPE.key(), "S1:1/1,0/1"),
                extractGenotypeFromFormatFilter(new Query(SAMPLE_DATA.key(), "S1:GT=1/1,0/1;DP>4")));
        assertEquals(new Query(SAMPLE_DATA.key(), "S1:PL<3;DP>4").append(GENOTYPE.key(), "S1:1/1,0/1"),
                extractGenotypeFromFormatFilter(new Query(SAMPLE_DATA.key(), "S1:PL<3;GT=1/1,0/1;DP>4")));
        assertEquals(new Query(SAMPLE_DATA.key(), "S1:PL<3;DP>4;S2:DP>3").append(GENOTYPE.key(), "S1:1/1,0/1;S2:1/1"),
                extractGenotypeFromFormatFilter(new Query(SAMPLE_DATA.key(), "S1:PL<3;GT=1/1,0/1;DP>4;S2:DP>3;GT=1/1")));
        assertEquals(new Query(SAMPLE_DATA.key(), "S1:PL<3;DP>4,S2:DP>3").append(GENOTYPE.key(), "S1:1/1,0/1,S2:1/1"),
                extractGenotypeFromFormatFilter(new Query(SAMPLE_DATA.key(), "S1:PL<3;GT=1/1,0/1;DP>4,S2:DP>3;GT=1/1")));
    }

    @Test
    public void testIncludeFormats() {
        checkIncludeFormats("GT", "GT");
        checkIncludeFormats("DP", "DP");
        checkIncludeFormats("GT,DP,AD", "GT,DP,AD");
        checkIncludeFormats("GT,DP", "DP,GT");
        checkIncludeFormats("GT,DP", "DP,GT,DP");
        checkIncludeFormats(null, ALL);
        checkIncludeFormats(null, "");
        checkIncludeFormats(null, false, "");
        checkIncludeFormats("GT", true, "");
        checkIncludeFormats("", NONE);
        checkIncludeFormats("GT", true, NONE);
        checkIncludeFormats("GT,DP,AD", true, "DP,AD");
        checkIncludeFormats("GT,DP,AD", true, "GT,DP,AD");
        checkIncludeFormats("DP,AD", false, "DP,AD");
        checkIncludeFormats("GT,DP,AD", true, "DP,AD");
    }

    private void checkIncludeFormats(String expected, boolean includeGenotype, String includeFormat) {
        checkIncludeFormats(expected, new Query(INCLUDE_SAMPLE_DATA.key(), includeFormat).append(INCLUDE_GENOTYPE.key(), includeGenotype));
        checkIncludeFormats(expected, new Query(INCLUDE_SAMPLE_DATA.key(), includeFormat.replace(',', ':')).append(INCLUDE_GENOTYPE.key(), includeGenotype));
    }

    private void checkIncludeFormats(String expected, String includeFormat) {
        checkIncludeFormats(expected, new Query(INCLUDE_SAMPLE_DATA.key(), includeFormat));
        checkIncludeFormats(expected, new Query(INCLUDE_SAMPLE_DATA.key(), includeFormat.replace(',', ':')));
    }

    private static void checkIncludeFormats(String expected, Query query) {
        List<String> expectedList = expected == null ? null
                : expected.isEmpty() ? Collections.emptyList() : Arrays.asList(expected.split(","));

        assertEquals(expectedList, getIncludeSampleData(query));
    }

    @Test
    public void mergeRegionsTest() throws Exception {
        assertEquals(Arrays.asList(new Region("1", 100, 400)), mergeRegions(Arrays.asList(new Region("1", 100, 200), new Region("1", 200, 400))));
        assertEquals(Arrays.asList(new Region("1", 100, 200), new Region("1", 201, 400)), mergeRegions(Arrays.asList(new Region("1", 100, 200), new Region("1", 201, 400))));
        assertEquals(Arrays.asList(new Region("1", 100, 200), new Region("2", 200, 400)), mergeRegions(Arrays.asList(new Region("1", 100, 200), new Region("2", 200, 400))));
        assertEquals(Arrays.asList(new Region("1", 100, 400), new Region("2", 100, 200)), mergeRegions(Arrays.asList(new Region("1", 100, 200), new Region("2", 100, 200), new Region("1", 200, 400))));
    }

    @Test
    public void checkAllPrivateParams() throws IllegalAccessException {
        Set<QueryParam> actualQueryParams = new HashSet<>();
        for (Field field : VariantQueryUtils.class.getFields()) {
            if (QueryParam.class.isAssignableFrom(field.getType())) {
                actualQueryParams.add((QueryParam) field.get(null));
            }
        }
        assertEquals(actualQueryParams, new HashSet<>(INTERNAL_VARIANT_QUERY_PARAMS));
    }

    @Test
    public void isVariantId() {
        checkIsVariantId("1:1000:A:T");
        checkIsVariantId("1:1000-2000:A:T");
        checkIsVariantId("1:1000-2000:A:<DUP:TANDEM>");
        checkIsVariantId("11:14525312:-:]11:14521700].");
        checkIsVariantId("4:100:C:[15:300[A");
        checkIsVariantId("4:100:C:]15:300]A");
        checkIsVariantId("rs123");
    }

    public void checkIsVariantId(String v) {

        boolean actual = VariantQueryUtils.isVariantId(v);
        boolean expected;
        try {
            new Variant(v);
            expected = true;
        } catch (Exception e) {
            expected = false;
        }
        assertEquals(v, expected, actual);
    }

}