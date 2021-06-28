package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.commons.lang3.tuple.Pair;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.io.bit.BitInputStream;
import org.opencb.opencga.storage.core.variant.query.OpValue;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;

import java.util.*;

public class CtBtCombinationIndexSchema /*extends DynamicIndexSchema*/ {

    private final Field field;

    public CtBtCombinationIndexSchema(ConsequenceTypeIndexSchema consequenceTypeIndexSchema, BiotypeIndexSchema biotypeIndexSchema) {
        field = new Field(consequenceTypeIndexSchema, biotypeIndexSchema);
    }

    public Field getField() {
        return field;
    }

    public class Field {
        private final ConsequenceTypeIndexSchema ctIndexSchema;
        private final BiotypeIndexSchema btIndexSchema;
        private final int ctValues;
        private final int btValues;

        public Field(ConsequenceTypeIndexSchema consequenceTypeIndexSchema, BiotypeIndexSchema biotypeIndexSchema) {
            this.ctIndexSchema = consequenceTypeIndexSchema;
            this.btIndexSchema = biotypeIndexSchema;
            ctValues = consequenceTypeIndexSchema.getField().getConfiguration().getValues().length + 1;
            btValues = biotypeIndexSchema.getField().getConfiguration().getValues().length + 1;
        }

        public AnnotationIndexEntry.CtBtCombination read(BitInputStream ctBtIndex, int ctIndex, int btIndex) {
            AnnotationIndexEntry.CtBtCombination ctBtCombination = new AnnotationIndexEntry.CtBtCombination(null, 0, 0);
            read(ctBtIndex, ctIndex, btIndex, ctBtCombination);
            return ctBtCombination;
        }

        public void read(BitInputStream ctBtIndex, int ctIndex, int btIndex,
                         AnnotationIndexEntry.CtBtCombination ctBtCombination) {
            int numCt = Integer.bitCount(ctIndex);
            int numBt = Integer.bitCount(btIndex);
            int[] ctBtMatrix = new int[numCt];
            for (int i = 0; i < numCt; i++) {
                ctBtMatrix[i] = ctBtIndex.readIntPartial(numBt);
            }
            ctBtCombination
                    .setNumCt(numCt)
                    .setNumBt(numBt)
                    .setCtBtMatrix(ctBtMatrix);
        }

        public BitBuffer write(List<Pair<String, String>> ctBtPairs) {
            Set<String> cts = new HashSet<>();
            Set<String> bts = new HashSet<>();

            for (Pair<String, String> ctBtPair : ctBtPairs) {
                cts.add(ctBtPair.getKey());
                bts.add(ctBtPair.getValue());
            }
            return encode(ctBtPairs,
                    ctIndexSchema.getField().write(new ArrayList<>(cts)),
                    btIndexSchema.getField().write(new ArrayList<>(bts)));
        }

        public BitBuffer encode(List<Pair<String, String>> ctBtPair, BitBuffer ctBuffer, BitBuffer btBuffer) {
            return encode(getCtBtCombination(ctBtPair, ctBuffer, btBuffer));
        }

        public BitBuffer encode(AnnotationIndexEntry.CtBtCombination ctBtCombination) {
            int numCt = ctBtCombination.getNumCt();
            int numBt = ctBtCombination.getNumBt();
            BitBuffer ctBtBuffer = new BitBuffer(numCt * numBt);
            int offset = 0;
            for (int i = 0; i < numCt; i++) {
                ctBtBuffer.setIntPartial(ctBtCombination.getCtBtMatrix()[i], offset, numBt);
                offset += numBt;
            }
            return ctBtBuffer;
        }

        public AnnotationIndexEntry.CtBtCombination getCtBtCombination(List<Pair<String, String>> ctBtPair,
                                                                       BitBuffer ctBuffer, BitBuffer btBuffer) {
            boolean[][] ctBt = new boolean[ctValues + 1][btValues + 1];
            for (Pair<String, String> pair : ctBtPair) {
                int ctEncode = ctIndexSchema.getField().encode(Collections.singletonList(pair.getKey()));
                int btEncode = btIndexSchema.getField().encode(Collections.singletonList(pair.getValue()));
                if (ctEncode == 0 || btEncode == 0) {
                    continue;
                }
                int ctPos = Integer.numberOfTrailingZeros(ctEncode);
                int btPos = Integer.numberOfTrailingZeros(btEncode);
                ctBt[ctPos][btPos] = true;
            }
            AnnotationIndexEntry.CtBtCombination ctBtCombination;
            int ctIndex = ctBuffer.toInt();
            int numCt = Integer.bitCount(ctIndex);
            int btIndex = btBuffer.toInt();
            int numBt = Integer.bitCount(btIndex);
            if (numBt > 0 && numCt > 0) {
                int[] ctBtMatrix = new int[numCt];

                for (int ctPos = 0; ctPos < numCt; ctPos++) {
                    // Get the first CT value from the right.
                    int ct = Integer.lowestOneBit(ctIndex);
                    // Remove the CT value from the index, so the next iteration gets the next value
                    ctIndex &= ~ct;
                    int btIndexAux = btIndex;
                    int combinationValue = 0;
                    for (int btPos = 0; btPos < numBt; btPos++) {
                        // As before, take the first BT value from the right.
                        int bt = Integer.lowestOneBit(btIndexAux);
                        btIndexAux &= ~bt;

                        // If the CT+BT combination is true, write a 1
                        if (ctBt[maskPosition(ct)][maskPosition(bt)]) {
                            combinationValue |= 1 << btPos;
                        }
                    }
                    ctBtMatrix[ctPos] = combinationValue;
                }

                ctBtCombination = new AnnotationIndexEntry.CtBtCombination(ctBtMatrix, numCt, numBt);
            } else {
                ctBtCombination = null;
            }
            return ctBtCombination;
        }

        private int maskPosition(int s) {
            return Integer.numberOfTrailingZeros(s);
        }

        public Filter buildFilter(List<Pair<String, String>> value) {
            Set<String> cts = new HashSet<>();
            Set<String> bts = new HashSet<>();

            for (Pair<String, String> ctBtPair : value) {
                cts.add(ctBtPair.getKey());
                bts.add(ctBtPair.getValue());
            }
            IndexFieldFilter ctFilter = ctIndexSchema.getField().buildFilter(new OpValue<>("=", new ArrayList<>(cts)));
            IndexFieldFilter btFilter = btIndexSchema.getField().buildFilter(new OpValue<>("=", new ArrayList<>(bts)));

            return buildFilter(ctFilter, btFilter);
        }

        public Filter buildFilter(IndexFieldFilter ctFilter, IndexFieldFilter btFilter) {
            return new Filter(ctFilter, btFilter);
        }

        public Filter noOpFilter() {
            return new Filter(null, null) {
                @Override
                public boolean test(AnnotationIndexEntry annotationIndexEntry) {
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

    public class Filter {

        private final IndexFieldFilter ctFilter;
        private final IndexFieldFilter btFilter;

        public Filter(IndexFieldFilter ctFilter, IndexFieldFilter btFilter) {
            this.ctFilter = ctFilter;
            this.btFilter = btFilter;
        }

        public boolean test(AnnotationIndexEntry annotationIndexEntry) {
            AnnotationIndexEntry.CtBtCombination ctBtCombination = annotationIndexEntry.getCtBtCombination();
            // Check ct+bt combinations
            int ctIndex = annotationIndexEntry.getCtIndex();
            int numCt = ctBtCombination.getNumCt();
            int numBt = ctBtCombination.getNumBt();
            // Pitfall!
            // if (numCt == 1 || numBt == 1) { return true; } This may not be true.
            // There could be missing rows/columns in the index
            int[] ctBtMatrix = ctBtCombination.getCtBtMatrix();
            // Check if any combination matches que query filter.
            // Walk through all values of CT and BT in this variant.
            // 3 conditions must meet:
            //  - The CT is part of the query filter
            //  - The BT is part of the query filter
            //  - The variant had the combination of both
            for (int ctIndexPos = 0; ctIndexPos < numCt; ctIndexPos++) {
                // Get the first CT value from the right.
                int ct = Integer.lowestOneBit(ctIndex);
                // Remove the CT value from the index, so the next iteration gets the next value
                ctIndex &= ~ct;
                // Check if the CT is part of the query filter
                if (ctFilter.test(ct)) {
                    // Iterate over the Biotype values
                    int btIndex = annotationIndexEntry.getBtIndex();
                    for (int btIndexPos = 0; btIndexPos < numBt; btIndexPos++) {
                        // As before, take the first BT value from the right.
                        int bt = Integer.lowestOneBit(btIndex);
                        btIndex &= ~bt;
                        // Check if the BT is part of the query filter
                        if (btFilter.test(bt)) {
                            // Check if this CT was together with this BT
                            if ((ctBtMatrix[ctIndexPos] & (1 << btIndexPos)) != 0) {
                                return true;
                            }
                        }
                    }
                    // assert btIndex == 0; // We should have removed all BT from the index
                }
            }
            // assert ctIndex == 0; // We should have removed all CT from the index

            // could not find any valid combination
            return false;
        }

        public boolean isExactFilter() {
            return ctFilter.isExactFilter() && btFilter.isExactFilter();
        }

        public boolean isNoOp() {
            return false;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("CtBtCombinationFilter{");
            sb.append("exact=").append(isExactFilter());
            sb.append('}');
            return sb.toString();
        }
    }
}
