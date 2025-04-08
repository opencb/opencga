package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Triple;
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
 * Each element has a set of triples (X, Y, Z), which defines a list of X, Y and Z indexed values.
 * This can be represented as a 3D matrix where, for each element in X, the rows are Y values in the element, the columns are Z values,
 * and the intersection is a boolean representing if that specific combination occurs in the element.
 * <pre>
 *       +------------+-----------+----+         +------------+-----------+----+
 *   X1  |  Z1        | Z2        | ...|     X2  |  Z1        | Z2        | ...|   .....
 * +-----+------------+-----------+----+   +-----+------------+-----------+----+
 * |  Y1 |  X1-Y1-Z1  | X1-Y1-Z2  |    |   |  Y1 |  X2-Y1-Z1  | X2-Y1-Z2  |    |
 * |  Y2 |  X1-Y2-Z1  | X1-Y2-Z2  |    |   |  Y2 |  X2-Y2-Z1  | X2-Y2-Z2  |    |
 * | ... |            |           | ...|   | ... |            |           | ...|
 * +-----+------------+-----------+----+   +-----+------------+-----------+----+
 *
 * </pre>
 *
 * This combination matrix stored in the {@link CombinationTriple} class.
 * The matrix itself is stored as an array of rows: matrix = {A1_row, A2_row, ...}
 * The max number of B elements is 32, as we use an array of integers.
 *
 * This combination matrix can be stored sequentially using {@link org.opencb.opencga.storage.core.io.bit.BitOutputStream}.
 *
 * Then, to read the matrix, first have to determine its size, by counting the number of unique A and B
 * values in the index (see {@link Integer#bitCount}).
 * The order of the rows and columns matches with the order of the bits within the index.
 */
public class CombinationTripleIndexSchema /*extends DynamicIndexSchema*/ {

    private final Field field;

    public CombinationTripleIndexSchema(CategoricalMultiValuedIndexField<String> indexX,
                                        CategoricalMultiValuedIndexField<String> indexY,
                                        CategoricalMultiValuedIndexField<String> indexZ) {
        field = new Field(indexX, indexY, indexZ);
    }

    public Field getField() {
        return field;
    }

    public static class Field {
        private final CategoricalMultiValuedIndexField<String> indexX;
        private final CategoricalMultiValuedIndexField<String> indexY;
        private final CategoricalMultiValuedIndexField<String> indexZ;
        private final int numValuesX;
        private final int numValuesY;
        private final int numValuesZ;

        public Field(CategoricalMultiValuedIndexField<String> indexX,
                     CategoricalMultiValuedIndexField<String> indexY,
                     CategoricalMultiValuedIndexField<String> indexZ) {
            this.indexX = indexX;
            this.indexY = indexY;
            this.indexZ = indexZ;
            numValuesX = indexX.getBitLength();
            numValuesY = indexY.getBitLength();
            numValuesZ = indexZ.getBitLength();
            if (numValuesX > Integer.SIZE || numValuesY > Integer.SIZE || numValuesZ > Integer.SIZE) {
                throw new IllegalArgumentException("Unable to build " + this.getClass().getSimpleName() + ". "
                        + "numValuesX = " + numValuesX + ", numValuesY = " + numValuesY + ", numValuesZ = " + numValuesZ);
            }
        }

        public CombinationTriple read(BitInputStream index, int indexFieldValueX, int indexFieldValueY, int indexFieldValueZ) {
            CombinationTriple combination = new CombinationTriple(null, 0, 0, 0);
            read(index, indexFieldValueX, indexFieldValueY, indexFieldValueZ, combination);
            return combination;
        }

        public void read(BitInputStream combinationIndex, int indexFieldValueX, int indexFieldValueY, int indexFieldValueZ,
                         CombinationTriple combination) {
            int numX = Integer.bitCount(indexFieldValueX);
            int numY = Integer.bitCount(indexFieldValueY);
            int numZ = Integer.bitCount(indexFieldValueZ);
            int[][] matrix = new int[numX][numY];
            for (int x = 0; x < numX; x++) {
                for (int y = 0; y < numY; y++) {
                    matrix[x][y] = combinationIndex.readIntPartial(numZ);
                }
            }
            combination
                    .setNumX(numX)
                    .setNumY(numY)
                    .setNumZ(numZ)
                    .setMatrix(matrix);
        }

        public BitBuffer write(List<Triple<String, String, String>> triples) {
            Set<String> setX = new HashSet<>();
            Set<String> setY = new HashSet<>();
            Set<String> setZ = new HashSet<>();

            for (Triple<String, String, String> triple : triples) {
                setX.add(triple.getLeft());
                setY.add(triple.getMiddle());
                setZ.add(triple.getRight());
            }
            return encode(triples,
                    indexX.write(new ArrayList<>(setX)),
                    indexY.write(new ArrayList<>(setY)),
                    indexZ.write(new ArrayList<>(setZ)));
        }

        public BitBuffer encode(List<Triple<String, String, String>> triples,
                                BitBuffer fieldBufferX,
                                BitBuffer fieldBufferY,
                                BitBuffer fieldBufferZ) {
            return encode(getCombination(triples, fieldBufferX, fieldBufferY, fieldBufferZ));
        }

        public BitBuffer encode(CombinationTriple combination) {
            int numX = combination.getNumX();
            int numY = combination.getNumY();
            int numZ = combination.getNumZ();
            BitBuffer bitBuffer = new BitBuffer(numX * numY * numZ);
            int offset = 0;
            for (int x = 0; x < numX; x++) {
                for (int y = 0; y < numY; y++) {
                    bitBuffer.setIntPartial(combination.getMatrix()[x][y], offset, numZ);
                    offset += numZ;
                }
            }
            return bitBuffer;
        }

        public CombinationTriple getCombination(List<Triple<String, String, String>> triples,
                                                BitBuffer fieldBufferX,
                                                BitBuffer fieldBufferY,
                                                BitBuffer fieldBufferZ) {
            boolean[][][] pairsMatrix = new boolean[numValuesX + 1][numValuesY + 1][numValuesZ + 1];
            for (Triple<String, String, String> triple : triples) {
                int encodeX = indexX.encode(Collections.singletonList(triple.getLeft()));
                int encodeY = indexY.encode(Collections.singletonList(triple.getMiddle()));
                int encodeZ = indexZ.encode(Collections.singletonList(triple.getRight()));
                if (encodeX == 0 || encodeY == 0 || encodeZ == 0) {
                    continue;
                }
                int posX = Integer.numberOfTrailingZeros(encodeX);
                int posY = Integer.numberOfTrailingZeros(encodeY);
                int posZ = Integer.numberOfTrailingZeros(encodeZ);
                pairsMatrix[posX][posY][posZ] = true;
            }
            CombinationTriple combination;
            final int fieldValueX = fieldBufferX.toInt();
            int numX = Integer.bitCount(fieldValueX);
            final int fieldValueY = fieldBufferY.toInt();
            int numY = Integer.bitCount(fieldValueY);
            final int fieldValueZ = fieldBufferZ.toInt();
            int numZ = Integer.bitCount(fieldValueZ);
            if (numX > 0 && numY > 0 && numZ > 0) {
                int[][] matrix = new int[numX][numY];

                int strippedFieldValueX = fieldValueX;
                for (int posX = 0; posX < numX; posX++) {
                    // Get the first A value from the right.
                    int x = Integer.lowestOneBit(strippedFieldValueX);
                    // Remove the A value from the index, so the next iteration gets the next value
                    strippedFieldValueX &= ~x;
                    int strippedFieldValueY = fieldValueY;
                    for (int posY = 0; posY < numY; posY++) {
                        // As before, take the first Y value from the right.
                        int y = Integer.lowestOneBit(strippedFieldValueY);
                        strippedFieldValueY &= ~y;


                        int combinationValue = 0;
                        // Iterate over Z values
                        int strippedFieldValueZ = fieldValueZ;
                        for (int posZ = 0; posZ < numZ; posZ++) {
                            // As before, take the first Z value from the right.
                            int z = Integer.lowestOneBit(strippedFieldValueZ);
                            strippedFieldValueZ &= ~z;

                            // If the A+B combination is true, write a 1
                            if (pairsMatrix[maskPosition(x)][maskPosition(y)][maskPosition(z)]) {
                                combinationValue |= 1 << posZ;
                            }
                        }
                        matrix[posX][posY] = combinationValue;
                    }
                }

                combination = new CombinationTriple(matrix, numX, numY, numZ);
            } else {
                combination = null;
            }
            return combination;
        }

        public List<Triple<String, String, String>> getTriples(CombinationTriple combination,
                                                               int fieldValueX, int fieldValueY, int fieldValueZ) {
            List<Triple<String, String, String>> triples = new LinkedList<>();

            int numX = combination.getNumX();
            int numY = combination.getNumY();
            int numZ = combination.getNumZ();

            int[][] matrix = combination.getMatrix();
            // Walk through all values of X, Y and Z in this value.
            int strippedFieldValueX = fieldValueX;
            for (int posX = 0; posX < numX; posX++) {
                // Get the first X value from the right.
                int x = Integer.lowestOneBit(strippedFieldValueX);
                String decodeX = indexX.decode(x).get(0);
                // Remove the X value from the index, so the next iteration gets the next value
                strippedFieldValueX &= ~x;

                // Iterate over Y values
                int strippedFieldValueY = fieldValueY;
                for (int posY = 0; posY < numY; posY++) {
                    // As before, take the first Y value from the right.
                    int y = Integer.lowestOneBit(strippedFieldValueY);
                    String decodeY = indexY.decode(y).get(0);
                    // Remove the Y value from the index, so the next iteration gets the next value
                    strippedFieldValueY &= ~y;


                    // Iterate over Z values
                    int strippedFieldValueZ = fieldValueZ;
                    for (int posZ = 0; posZ < numZ; posZ++) {
                        // As before, take the first Z value from the right.
                        int z = Integer.lowestOneBit(strippedFieldValueZ);
                        strippedFieldValueZ &= ~z;

                        // Check if this X and Y was together with this Z
                        if ((matrix[posX][posY] & (1 << posZ)) != 0) {
                            String decodeZ = indexZ.decode(z).get(0);
                            triples.add(Triple.of(
                                    decodeX == null ? "OTHER" : decodeX,
                                    decodeY == null ? "OTHER" : decodeY,
                                    decodeZ == null ? "OTHER" : decodeZ));
                        }
                    }
                    // assert strippedFieldValueZ == 0; // We should have removed all B values from the index
                }
                // assert strippedFieldValueY == 0; // We should have removed all B values from the index
            }
            // assert strippedFieldValueX == 0; // We should have removed all A values from the index

            return triples;
        }

        private int maskPosition(int s) {
            return Integer.numberOfTrailingZeros(s);
        }

//        public Filter buildFilter(List<Triple<String, String, String>> value) {
//            Set<String> setX = new HashSet<>();
//            Set<String> setY = new HashSet<>();
//            Set<String> setZ = new HashSet<>();
//
//            for (Triple<String, String, String> triple : value) {
//                setX.add(triple.getLeft());
//                setY.add(triple.getMiddle());
//                setZ.add(triple.getRight());
//            }
//            return buildFilter(setX, setY, setZ);
//        }

        public Filter buildFilter(Collection<String> xValues, Collection<String> yValues, Collection<String> zValues) {
            IndexFieldFilter filterX = CollectionUtils.isEmpty(xValues) ? indexX.noOpFilter()
                    : indexX.buildFilter(new OpValue<>("=", new ArrayList<>(xValues)));
            IndexFieldFilter filterY = CollectionUtils.isEmpty(yValues) ? indexY.noOpFilter()
                    : indexY.buildFilter(new OpValue<>("=", new ArrayList<>(yValues)));
            IndexFieldFilter filterZ = CollectionUtils.isEmpty(zValues) ? indexZ.noOpFilter()
                    : indexZ.buildFilter(new OpValue<>("=", new ArrayList<>(zValues)));

            return buildFilter(filterX, filterY, filterZ);
        }

        public Filter buildFilter(IndexFieldFilter filterX, IndexFieldFilter filterY, IndexFieldFilter filterZ) {
            return new Filter(filterX, filterY, filterZ);
        }

        public Filter noOpFilter() {
            return new Filter(null, null, null) {
                @Override
                public boolean test(CombinationTriple combination, int fieldValueX, int fieldValueY, int fieldValueZ) {
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

        public CategoricalMultiValuedIndexField<String> getIndexX() {
            return indexX;
        }

        public CategoricalMultiValuedIndexField<String> getIndexY() {
            return indexY;
        }

        public CategoricalMultiValuedIndexField<String> getIndexZ() {
            return indexZ;
        }
    }

    public static class Filter {

        private final IndexFieldFilter filterX;
        private final IndexFieldFilter filterY;
        private final IndexFieldFilter filterZ;

        public Filter(IndexFieldFilter filterX, IndexFieldFilter filterY, IndexFieldFilter filterZ) {
            this.filterX = filterX;
            this.filterY = filterY;
            this.filterZ = filterZ;
        }

        public boolean test(CombinationTriple combination, final int fieldValueX, final int fieldValueY, final int fieldValueZ) {
            // Check XYZ combinations
            int numX = combination.getNumX();
            int numY = combination.getNumY();
            int numZ = combination.getNumZ();
            // Pitfall!
            // if (numX == 1 || numY == 1 || numZ == 1) { return true; } This may not be true.
            // There could be missing rows/columns in the index
            int[][] matrix = combination.getMatrix();
            // Check if any combination matches que query filter.
            // Walk through all values of X, Y and Z in this value.
            // 4 conditions must meet:
            //  - The value X is part of the query filter
            //  - The value Y is part of the query filter
            //  - The value Z is part of the query filter
            //  - The element had the combination of all three
            int strippedFieldValueX = fieldValueX;
            for (int posX = 0; posX < numX; posX++) {
                // Get the first X value from the right.
                int x = Integer.lowestOneBit(strippedFieldValueX);
                // Remove the X value from the index, so the next iteration gets the next value
                strippedFieldValueX &= ~x;
                // Check if the X is part of the query filter
                if (filterX.test(x)) {
                    // Iterate over Y values
                    int strippedFieldValueY = fieldValueY;
                    for (int posY = 0; posY < numY; posY++) {
                        // As before, take the first Y value from the right.
                        int y = Integer.lowestOneBit(strippedFieldValueY);
                        strippedFieldValueY &= ~y;
                        // Check if the Y is part of the query filter
                        if (filterY.test(y)) {

                            // Iterate over Z values
                            int strippedFieldValueZ = fieldValueZ;
                            for (int posZ = 0; posZ < numZ; posZ++) {
                                // As before, take the first Z value from the right.
                                int z = Integer.lowestOneBit(strippedFieldValueZ);
                                strippedFieldValueZ &= ~z;

                                // Check if the Z is part of the query filter
                                if (filterZ.test(z)) {

                                    // Check if this X and Y was together with this Z
                                    if ((matrix[posX][posY] & (1 << posZ)) != 0) {
                                        return true;
                                    }
                                }
                            }
                            // assert strippedFieldValueZ == 0; // We should have removed all Z values from the index
                        }
                    }
                    // assert strippedFieldValueY == 0; // We should have removed all Y values from the index
                }
            }
            // assert strippedFieldValueX == 0; // We should have removed all X values from the index

            // could not find any valid combination
            return false;
        }

        public boolean isExactFilter() {
            return (filterX.isExactFilter() || filterX.isNoOp())
                    && (filterY.isExactFilter() || filterY.isNoOp())
                    && (filterZ.isExactFilter() || filterZ.isNoOp());
        }

        public boolean isNoOp() {
            return false;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("CombinationTripleFilter{");
            sb.append("filterX=").append(filterX);
            sb.append(", filterY=").append(filterY);
            sb.append(", filterZ=").append(filterZ);
            sb.append(", exact=").append(isExactFilter());
            sb.append('}');
            return sb.toString();
        }
    }

    public static class CombinationTriple {
        private int[][] matrix;
        private int numX;
        private int numY;
        private int numZ;

        public CombinationTriple() {
            this(new int[0][0], 0, 0, 0);
        }

        public CombinationTriple(int[][] matrix, int numX, int numY, int numZ) {
            this.matrix = matrix;
            this.numX = numX;
            this.numY = numY;
            this.numZ = numZ;
        }

        public CombinationTriple(CombinationTriple other) {
            this.matrix = Arrays.copyOf(other.matrix, other.matrix.length);
            this.numX = other.numX;
            this.numY = other.numY;
            this.numZ = other.numZ;
        }

        public int[][] getMatrix() {
            return matrix;
        }

        public CombinationTriple setMatrix(int[][] matrix) {
            this.matrix = matrix;
            return this;
        }

        public int getNumX() {
            return numX;
        }

        public CombinationTriple setNumX(int numX) {
            this.numX = numX;
            return this;
        }

        public int getNumY() {
            return numY;
        }

        public CombinationTriple setNumY(int numY) {
            this.numY = numY;
            return this;
        }

        public int getNumZ() {
            return numZ;
        }

        public CombinationTriple setNumZ(int numZ) {
            this.numZ = numZ;
            return this;
        }

        public static CombinationTriple empty() {
            return new CombinationTriple();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("CombinationTriple{");

            sb.append("numX=").append(numX);
            sb.append(", numY=").append(numY);
            sb.append(", numZ=").append(numZ);
            sb.append(", matrix=").append(Arrays.stream(matrix)
                    .map(a -> IntStream.of(a)
                            .mapToObj(i -> IndexUtils.binaryToString(i, numY))
                            .collect(Collectors.joining(", ", "[", "]")))
                    .collect(Collectors.joining(", ", "[", "]")));
            sb.append('}');
            return sb.toString();
        }
    }
}
