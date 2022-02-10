package org.opencb.opencga.storage.core.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BatchUtils {


    /**
     * Split list in evenly distributed batches.
     *
     * @param list List of elements to split.
     * @param maxBatchSize Max size of each batch.
     * @param <T> Type of element
     * @return List of split batches
     */
    public static <T> List<List<T>> splitBatches(List<T> list, int maxBatchSize) {
        return splitBatches(list, maxBatchSize, false);
    }

    /**
     * Split list in evenly distributed batches.
     *
     * @param list List of elements to split.
     * @param maxBatchSize Max size of each batch.
     * @param copyBatchList Create a copy for each batch instead of using a sub-list reference.
     * @param <T> Type of element
     * @return List of split batches
     */
    public static <T> List<List<T>> splitBatches(List<T> list, int maxBatchSize, boolean copyBatchList) {
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("Illegal max batch size: " + maxBatchSize);
        }
        if (list.isEmpty()) {
            return Collections.emptyList();
        }
        int batches = ceilDiv(list.size(), maxBatchSize);
        int batchSize = ceilDiv(list.size(), batches);
        List<List<T>> parts = new ArrayList<>(batches);
        for (int i = 0; i < batches; i++) {
            List<T> batch = list.subList(i * batchSize, Math.min((i + 1) * batchSize, list.size()));
            if (copyBatchList) {
                batch = new ArrayList<>(batch);
            }
            parts.add(batch);
        }
        return parts;
    }

    /**
     * Returns the largest (closest to positive infinity) {@code int} value that is greater than or equal to the algebraic quotient.
     *
     * @param a the dividend
     * @param b the divisor
     * @return the largest (closest to positive infinity) {@code int} value that is less than or equal to the algebraic quotient.
     */
    private static int ceilDiv(int a, int b) {
        return (a + b - 1) / b;
//        return (int) Math.round(Math.ceil(a / (float) b));
    }

}
