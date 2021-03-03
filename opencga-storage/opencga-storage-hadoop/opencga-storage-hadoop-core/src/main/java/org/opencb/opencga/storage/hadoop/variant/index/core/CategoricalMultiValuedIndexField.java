package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.storage.core.variant.query.OpValue;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.MaskIndexFieldFilter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Categorical multi valued index.
 * <p>
 * Each value will be codified in one single position of the array.
 * Total number of bits would be the number of values.
 * Repetitions are not counted.
 * Value "0" represents NA.
 *
 * @param <T> Value type
 */
public class CategoricalMultiValuedIndexField<T> extends CategoricalIndexField<List<T>> {

    private final int numBits;

    public CategoricalMultiValuedIndexField(IndexFieldConfiguration configuration, int bitOffset, T... values) {
        super(configuration, bitOffset, values.length, new MaskValueCodec<>(values));
        numBits = values.length;
    }

    @Override
    protected IndexFieldFilter getOrIndexFilter(List<OpValue<List<T>>> opValues) {
        opValues.forEach(this::checkOperator);
        List<T> list = opValues.stream().flatMap(v -> v.getValue().stream()).collect(Collectors.toList());
        return new MaskIndexFieldFilter(this, list);
    }

    @Override
    protected IndexFieldFilter getSingleValueIndexFilter(OpValue<List<T>> opValue) {
        checkOperator(opValue);
        return new MaskIndexFieldFilter(this, opValue.getValue());
    }

    @Override
    public int getBitLength() {
        return numBits;
    }

    /**
     * This codec will generate a number with as many bits as possible values.
     *
     * @param <T> Value type
     */
    private static class MaskValueCodec<T> implements IndexCodec<List<T>> {

        private final T[] values;

        MaskValueCodec(T[] values) {
            this.values = values;
            if (values.length > Integer.SIZE) {
                throw new IllegalArgumentException("Unable to represent more than " + Integer.SIZE
                        + " values in a " + CategoricalMultiValuedIndexField.class.getSimpleName());
            }
        }

        @Override
        public int encode(List<T> value) {
            int code = 0;
            for (int i = 0, valuesLength = values.length; i < valuesLength; i++) {
                T t = values[i];
                if (t.equals(value)) {
                    code |= 1 << i;
                }
            }
            return code;
        }

        @Override
        public List<T> decode(int code) {
            if (code == 0) {
                return Collections.emptyList();
            } else {
                List<T> decode = new ArrayList<>(values.length);
                for (int i = 0; i < values.length; i++) {
                    if ((code & 1) == 1) {
                        decode.add(values[i]);
                    }
                    code = code >>> 1;
                }
                return decode;
            }
        }

        @Override
        public boolean ambiguous(int code) {
            return code == NA;
        }
    }

}
