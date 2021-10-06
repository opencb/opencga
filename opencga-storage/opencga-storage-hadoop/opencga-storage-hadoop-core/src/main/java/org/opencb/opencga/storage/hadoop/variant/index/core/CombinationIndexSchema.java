package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.apache.commons.lang3.tuple.Pair;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.io.bit.BitInputStream;
import org.opencb.opencga.storage.core.variant.query.OpValue;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Index fields combination index.
 *
 * Each element has a set of pairs (A, B), which defines a list of A and B indexed values.
 * This can be represented as a matrix where the rows are A values in the element, the columns are B values,
 * and the intersection is a boolean representing if that specific combination occurs in the element.
 * <pre>
 *       +---------+--------+----+
 *       |  B1     | B2     | ...|
 * +-----+---------+--------+----+
 * |  A1 |  A1-B1  | A1-B2  |    |
 * |  A2 |  A2-B1  | A2-B2  |    |
 * | ... |         |        | ...|
 * +-----+---------+--------+----+
 * </pre>
 *
 * This combination matrix stored in the {@link Combination} class.
 * The matrix itself is stored as an array of rows: matrix = {A1_row, A2_row, ...}
 * The max number of B elements is 32, as we use an array of integers.
 *
 * This combination matrix can be stored sequentially using {@link org.opencb.opencga.storage.core.io.bit.BitOutputStream}.
 *
 * Then, to read the matrix, first have to determine its size, by counting the number of unique A and B
 * values in the index (see {@link Integer#bitCount}).
 * The order of the rows and columns matches with the order of the bits within the index.
 */
public class CombinationIndexSchema /*extends DynamicIndexSchema*/ {

    private final Field field;

    public CombinationIndexSchema(CategoricalMultiValuedIndexField<String> indexA, CategoricalMultiValuedIndexField<String> indexB) {
        field = new Field(indexA, indexB);
    }

    public Field getField() {
        return field;
    }

    public static class Field {
        private final CategoricalMultiValuedIndexField<String> indexA;
        private final CategoricalMultiValuedIndexField<String> indexB;
        private final int numValuesA;
        private final int numValuesB;

        public Field(CategoricalMultiValuedIndexField<String> indexA, CategoricalMultiValuedIndexField<String> indexB) {
            this.indexA = indexA;
            this.indexB = indexB;
            numValuesA = indexA.getBitLength();
            numValuesB = indexB.getBitLength();
            if (numValuesA > Integer.SIZE || numValuesB > Integer.SIZE) {
                throw new IllegalArgumentException("Unable to build CombinationIndexSchema. "
                        + "numValuesA = " + numValuesA + ", numValuesB = " + numValuesB);
            }
        }

        public Combination read(BitInputStream index, int indexFieldValueA, int indexFieldValueB) {
            Combination combination = new Combination(null, 0, 0);
            read(index, indexFieldValueA, indexFieldValueB, combination);
            return combination;
        }

        public void read(BitInputStream combinationIndex, int indexFieldValueA, int indexFieldValueB,
                         Combination combination) {
            int numA = Integer.bitCount(indexFieldValueA);
            int numB = Integer.bitCount(indexFieldValueB);
            int[] matrix = new int[numA];
            for (int i = 0; i < numA; i++) {
                matrix[i] = combinationIndex.readIntPartial(numB);
            }
            combination
                    .setNumA(numA)
                    .setNumB(numB)
                    .setMatrix(matrix);
        }

        public BitBuffer write(List<Pair<String, String>> pairs) {
            Set<String> setA = new HashSet<>();
            Set<String> setB = new HashSet<>();

            for (Pair<String, String> pair : pairs) {
                setA.add(pair.getKey());
                setB.add(pair.getValue());
            }
            return encode(pairs,
                    indexA.write(new ArrayList<>(setA)),
                    indexB.write(new ArrayList<>(setB)));
        }

        public BitBuffer encode(List<Pair<String, String>> pairs, BitBuffer fieldBufferA, BitBuffer fieldBufferB) {
            return encode(getCombination(pairs, fieldBufferA, fieldBufferB));
        }

        public BitBuffer encode(Combination combination) {
            int numA = combination.getNumA();
            int numB = combination.getNumB();
            BitBuffer bitBuffer = new BitBuffer(numA * numB);
            int offset = 0;
            for (int i = 0; i < numA; i++) {
                bitBuffer.setIntPartial(combination.getMatrix()[i], offset, numB);
                offset += numB;
            }
            return bitBuffer;
        }

        public Combination getCombination(List<Pair<String, String>> pairs,
                                          BitBuffer fieldBufferA, BitBuffer fieldBufferB) {
            boolean[][] pairsMatrix = new boolean[numValuesA + 1][numValuesB + 1];
            for (Pair<String, String> pair : pairs) {
                int encodeA = indexA.encode(Collections.singletonList(pair.getKey()));
                int encodeB = indexB.encode(Collections.singletonList(pair.getValue()));
                if (encodeA == 0 || encodeB == 0) {
                    continue;
                }
                int posA = Integer.numberOfTrailingZeros(encodeA);
                int posB = Integer.numberOfTrailingZeros(encodeB);
                pairsMatrix[posA][posB] = true;
            }
            Combination combination;
            final int fieldValueA = fieldBufferA.toInt();
            int numA = Integer.bitCount(fieldValueA);
            final int fieldValueB = fieldBufferB.toInt();
            int numB = Integer.bitCount(fieldValueB);
            if (numB > 0 && numA > 0) {
                int[] matrix = new int[numA];

                int strippedFieldValueA = fieldValueA;
                for (int posA = 0; posA < numA; posA++) {
                    // Get the first A value from the right.
                    int a = Integer.lowestOneBit(strippedFieldValueA);
                    // Remove the A value from the index, so the next iteration gets the next value
                    strippedFieldValueA &= ~a;
                    int strippedFieldValueB = fieldValueB;
                    int combinationValue = 0;
                    for (int posB = 0; posB < numB; posB++) {
                        // As before, take the first B value from the right.
                        int b = Integer.lowestOneBit(strippedFieldValueB);
                        strippedFieldValueB &= ~b;

                        // If the A+B combination is true, write a 1
                        if (pairsMatrix[maskPosition(a)][maskPosition(b)]) {
                            combinationValue |= 1 << posB;
                        }
                    }
                    matrix[posA] = combinationValue;
                }

                combination = new Combination(matrix, numA, numB);
            } else {
                combination = null;
            }
            return combination;
        }

        public List<Pair<String, String>> getPairs(Combination combination, int fieldValueA, int fieldValueB) {
            List<Pair<String, String>> pairs = new LinkedList<>();

            int numA = combination.getNumA();
            int numB = combination.getNumB();

            int[] matrix = combination.getMatrix();
            // Walk through all values of A and B in this value.
            int strippedFieldValueA = fieldValueA;
            for (int posA = 0; posA < numA; posA++) {
                // Get the first A value from the right.
                int a = Integer.lowestOneBit(strippedFieldValueA);
                String decodeA = indexA.decode(a).get(0);
                // Remove the A value from the index, so the next iteration gets the next value
                strippedFieldValueA &= ~a;

                // Iterate over B values
                int strippedFieldValueB = fieldValueB;
                for (int posB = 0; posB < numB; posB++) {
                    // As before, take the first B value from the right.
                    int b = Integer.lowestOneBit(strippedFieldValueB);
                    strippedFieldValueB &= ~b;
                    // Check if this A was together with this B
                    if ((matrix[posA] & (1 << posB)) != 0) {
                        String decodeB = indexB.decode(b).get(0);
                        pairs.add(Pair.of(decodeA == null ? "OTHER" : decodeA, decodeB == null ? "OTHER" : decodeB));
                    }
                }
                // assert strippedFieldValueB == 0; // We should have removed all B values from the index
            }
            // assert strippedFieldValueA == 0; // We should have removed all A values from the index

            return pairs;
        }

        private int maskPosition(int s) {
            return Integer.numberOfTrailingZeros(s);
        }

//        public Filter buildFilter(List<Pair<String, String>> value) {
//            Set<String> setA = new HashSet<>();
//            Set<String> setB = new HashSet<>();
//
//            for (Pair<String, String> pair : value) {
//                setA.add(pair.getKey());
//                setB.add(pair.getValue());
//            }
//            return buildFilter(setA, setB);
//        }

        public Filter buildFilter(Collection<String> valuesA, Collection<String> valuesB) {
            IndexFieldFilter filterA = indexA.buildFilter(new OpValue<>("=", new ArrayList<>(valuesA)));
            IndexFieldFilter filterB = indexB.buildFilter(new OpValue<>("=", new ArrayList<>(valuesB)));

            return buildFilter(filterA, filterB);
        }

        public Filter buildFilter(IndexFieldFilter filterA, IndexFieldFilter filterB) {
            return new Filter(filterA, filterB);
        }

        public Filter noOpFilter() {
            return new Filter(null, null) {
                @Override
                public boolean test(Combination combination, int fieldValueA, int fieldValueB) {
                    return true;
                }

                @Override
                public boolean isExactFilter() {
                    return false;
                }

                @Override
                public boolean isNoOp() {
                    return true;
                }
            };
        }

    }

    public static class Filter {

        private final IndexFieldFilter filterA;
        private final IndexFieldFilter filterB;

        public Filter(IndexFieldFilter filterA, IndexFieldFilter filterB) {
            this.filterA = filterA;
            this.filterB = filterB;
        }

        public boolean test(Combination combination, final int fieldValueA, final int fieldValueB) {
            // Check AB combinations
            int numA = combination.getNumA();
            int numB = combination.getNumB();
            // Pitfall!
            // if (numA == 1 || numB == 1) { return true; } This may not be true.
            // There could be missing rows/columns in the index
            int[] matrix = combination.getMatrix();
            // Check if any combination matches que query filter.
            // Walk through all values of A and B in this value.
            // 3 conditions must meet:
            //  - The value A is part of the query filter
            //  - The value B is part of the query filter
            //  - The element had the combination of both
            int strippedFieldValueA = fieldValueA;
            for (int posA = 0; posA < numA; posA++) {
                // Get the first A value from the right.
                int a = Integer.lowestOneBit(strippedFieldValueA);
                // Remove the A value from the index, so the next iteration gets the next value
                strippedFieldValueA &= ~a;
                // Check if the A is part of the query filter
                if (filterA.test(a)) {
                    // Iterate over B values
                    int strippedFieldValueB = fieldValueB;
                    for (int posB = 0; posB < numB; posB++) {
                        // As before, take the first B value from the right.
                        int b = Integer.lowestOneBit(strippedFieldValueB);
                        strippedFieldValueB &= ~b;
                        // Check if the B is part of the query filter
                        if (filterB.test(b)) {
                            // Check if this A was together with this B
                            if ((matrix[posA] & (1 << posB)) != 0) {
                                return true;
                            }
                        }
                    }
                    // assert strippedFieldValueB == 0; // We should have removed all B values from the index
                }
            }
            // assert strippedFieldValueA == 0; // We should have removed all A values from the index

            // could not find any valid combination
            return false;
        }

        public boolean isExactFilter() {
            return filterA.isExactFilter() && filterB.isExactFilter();
        }

        public boolean isNoOp() {
            return false;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("CombinationFilter{");
            sb.append("exact=").append(isExactFilter());
            sb.append('}');
            return sb.toString();
        }
    }

    public static class Combination {
        private int[] matrix;
        private int numA;
        private int numB;

        public Combination() {
            this(new int[0], 0, 0);
        }

        public Combination(int[] matrix, int numA, int numB) {
            this.matrix = matrix;
            this.numA = numA;
            this.numB = numB;
        }

        public Combination(Combination other) {
            this.matrix = Arrays.copyOf(other.matrix, other.matrix.length);
            this.numA = other.numA;
            this.numB = other.numB;
        }

        public int[] getMatrix() {
            return matrix;
        }

        public Combination setMatrix(int[] matrix) {
            this.matrix = matrix;
            return this;
        }

        public int getNumA() {
            return numA;
        }

        public Combination setNumA(int numA) {
            this.numA = numA;
            return this;
        }

        public int getNumB() {
            return numB;
        }

        public Combination setNumB(int numB) {
            this.numB = numB;
            return this;
        }

        public static Combination empty() {
            return new Combination(new int[0], 0, 0);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Combination{");
            sb.append("numA=").append(numA);
            sb.append(", numB=").append(numB);
            sb.append(", matrix=").append(IntStream.of(matrix)
                    .mapToObj(i -> IndexUtils.binaryToString(i, numB))
                    .collect(Collectors.joining(", ", "[", "]")));
            sb.append('}');
            return sb.toString();
        }
    }
}
