package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Test;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.io.bit.BitInputStream;

import java.util.*;

import static org.junit.Assert.*;

public class CombinationTripleIndexSchemaTest {

    @Test
    public void test() {
        CombinationTripleIndexSchema schema = new CombinationTripleIndexSchema(
                buildIndex("X", false, "a", "b", "c", "d", "e", "f", "g", "h", "i"),
                buildIndex("Y", true, "1", "2", "3", "4", "5", "6", "7", "8", "9"),
                buildIndex("Z", true, "A", "B", "C", "D", "E", "F", "G"));

        test(schema, Arrays.asList(Triple.of("a", "3", "C")));
        test(schema, Arrays.asList(Triple.of("f", "3", "C"), Triple.of("a", "6", "D")));
        for (int R = 0; R < 20; R++) {
            for (int r = 1; r < 15; r++) {
                ArrayList<Triple<String, String, String>> triples = new ArrayList<>();
                for (int i = 0; i < r * 2; i++) {
                    triples.add(randomTriple(schema));
                }
                test(schema, triples);
            }
            for (int r = 1; r < 15; r++) {
                Set<String> extraX = new HashSet<>();
                Set<String> extraY = new HashSet<>();
                Set<String> extraZ = new HashSet<>();
                ArrayList<Triple<String, String, String>> triples = new ArrayList<>();
                for (int i = 0; i < r * 2; i++) {
                    triples.add(randomTriple(schema));
                    extraX.add(randomTriple(schema).getLeft());
                    extraY.add(randomTriple(schema).getMiddle());
                    extraZ.add(randomTriple(schema).getRight());
                }
                test(schema, triples, extraX, extraY, extraZ);
            }
        }

    }

    private Triple<String, String, String> randomTriple(CombinationTripleIndexSchema schema) {
        return Triple.of(
                randomPeek(schema.getField().getIndexX()),
                randomPeek(schema.getField().getIndexY()),
                randomPeek(schema.getField().getIndexZ())
        );
    }

    private String randomPeek(CategoricalMultiValuedIndexField<String> field) {
        String[] values = field.getConfiguration().getValues();
        if (field.getConfiguration().getNullable()) {
            if (RandomUtils.nextInt(0, values.length + 1) == values.length) {
                return "OTHER";
            }
        }
        return values[RandomUtils.nextInt(0, values.length)];
    }

    private void test(CombinationTripleIndexSchema schema, List<Triple<String, String, String>> expected) {
        test(schema, expected, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    private void test(CombinationTripleIndexSchema schema, List<Triple<String, String, String>> expected,
                      Collection<String> extraValuesX, Collection<String> extraValuesY, Collection<String> extraValuesZ) {
        Collections.sort(expected);

        Set<String> setX = new HashSet<>(extraValuesX);
        Set<String> setY = new HashSet<>(extraValuesY);
        Set<String> setZ = new HashSet<>(extraValuesZ);

        for (Triple<String, String, String> triple : expected) {
            setX.add(triple.getLeft());
            setY.add(triple.getMiddle());
            setZ.add(triple.getRight());
        }

        BitBuffer x = schema.getField().getIndexX().write(new ArrayList<>(setX));
        BitBuffer y = schema.getField().getIndexY().write(new ArrayList<>(setY));
        BitBuffer z = schema.getField().getIndexZ().write(new ArrayList<>(setZ));
        BitBuffer buff = schema.getField().encode(expected, x, y, z);

        // Test build combination, write, read and getTrios
        CombinationTripleIndexSchema.CombinationTriple generatedCombination = schema.getField().getCombination(expected, x, y, z);
        CombinationTripleIndexSchema.CombinationTriple readCombination = schema.getField().read(new BitInputStream(buff), x.toInt(), y.toInt(), z.toInt());
        List<Triple<String, String, String>> actual = schema.getField().getTriples(readCombination, x.toInt(), y.toInt(), z.toInt());

//        System.out.println(buff.getByteLength() + "B " + buff.getBitLength() + "b - triples = " + expected);
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));


        // Test filter
        for (int i = 1; i < 10; i++) {
            Collection<String> xFilters = new HashSet<>();
            Collection<String> yFilters = new HashSet<>();
            Collection<String> zFilters = new HashSet<>();

            for (int i1 = 0; i1 < i; i1++) {
                Triple<String, String, String> triple = randomTriple(schema);
                xFilters.add(triple.getLeft());
                yFilters.add(triple.getMiddle());
                zFilters.add(triple.getRight());
            }

            testFilter(schema, expected, x, y, z, readCombination, xFilters, yFilters, zFilters);
            testFilter(schema, expected, x, y, z, readCombination, xFilters, yFilters, null);
            testFilter(schema, expected, x, y, z, readCombination, xFilters, null, zFilters);
            testFilter(schema, expected, x, y, z, readCombination, xFilters, null, null);
            testFilter(schema, expected, x, y, z, readCombination, null, yFilters, zFilters);
            testFilter(schema, expected, x, y, z, readCombination, null, yFilters, null);
            testFilter(schema, expected, x, y, z, readCombination, null, null, zFilters);
        }

    }

    private void testFilter(CombinationTripleIndexSchema schema, List<Triple<String, String, String>> expected,
                            BitBuffer x, BitBuffer y, BitBuffer z, CombinationTripleIndexSchema.CombinationTriple readCombination,
                            Collection<String> xFilters, Collection<String> yFilters, Collection<String> zFilters) {
        CombinationTripleIndexSchema.Filter filter = schema.getField().buildFilter(xFilters, yFilters, zFilters);

        boolean expectedTestResult = false;
        Set<Triple<String, String, String>> filterQuery = new TreeSet<>();
        if (xFilters == null) {
            xFilters = new ArrayList<>(Arrays.asList(schema.getField().getIndexX().getConfiguration().getValues()));
            if (schema.getField().getIndexX().getConfiguration().getNullable()) {
                xFilters.add("OTHER");
            }
        }
        if (yFilters == null) {
            yFilters = new ArrayList<>(Arrays.asList(schema.getField().getIndexY().getConfiguration().getValues()));
            if (schema.getField().getIndexY().getConfiguration().getNullable()) {
                yFilters.add("OTHER");
            }
        }
        if (zFilters == null) {
            zFilters = new ArrayList<>(Arrays.asList(schema.getField().getIndexZ().getConfiguration().getValues()));
            if (schema.getField().getIndexZ().getConfiguration().getNullable()) {
                zFilters.add("OTHER");
            }
        }
        for (String xFilter : xFilters) {
            for (String yFilter : yFilters) {
                for (String zFilter : zFilters) {
                    Triple<String, String, String> triple = Triple.of(xFilter, yFilter, zFilter);
                    filterQuery.add(triple);
                    if (expected.contains(triple)) {
                        expectedTestResult = true;
                    }
                }
            }
        }

//        System.out.println(expectedTestResult + " filterQuery = " + filterQuery);
        boolean actualTestResult = filter.test(readCombination, x.toInt(), y.toInt(), z.toInt());
//            System.out.println("expectedTestResult = " + expectedTestResult);
//            System.out.println("actualTestResult = " + actualTestResult);
        assertEquals(expectedTestResult, actualTestResult);
    }

    private CategoricalMultiValuedIndexField<String> buildIndex(String name, boolean nullable, String... values) {

        return new CategoricalMultiValuedIndexField<>(new IndexFieldConfiguration(IndexFieldConfiguration.Source.ANNOTATION,
                name,
                IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE, values).setNullable(nullable), 0, values);
    }
}