package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.storage.core.variant.query.OpValue;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.MultiValueIndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.SingleValueIndexFieldFilter;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Single index categorical index.
 * <p>
 * Each value is codified with a unique number.
 * Total number of bits would be ceil(log2(values.length)).
 * Value "0" represents NA.
 */
public class CategoricalIndexField<T> extends IndexField<T> implements IndexCodec<T> {
    public static final int NA = 0;
    private final int numBits;
    private final IndexCodec<T> codec;

    public static CategoricalIndexField<String> create(IndexFieldConfiguration configuration, int bitOffset) {
        return new CategoricalIndexField<>(configuration, bitOffset, configuration.getValues());
    }

    public CategoricalIndexField(IndexFieldConfiguration configuration, int bitOffset, T[] values) {
        super(configuration, bitOffset);
        int numValues;
        if (configuration.getNullable()) {
            numValues = values.length + 1;
            codec = new BasicCodecWithNa<>(values);
        } else {
            numValues = values.length;
            codec = new BasicCodec<>(values);
        }
        this.numBits = Math.max(1, IndexUtils.log2(numValues - 1) + 1);
    }

    public CategoricalIndexField(IndexFieldConfiguration configuration, int bitOffset, int numValues, IndexCodec<T> codec) {
        super(configuration, bitOffset);
        this.numBits = IndexUtils.log2(numValues - 1) + 1;
        this.codec = codec;
    }

    @Override
    public int getBitLength() {
        return numBits;
    }

    @Override
    protected IndexFieldFilter getOrIndexFilter(List<OpValue<T>> values) {
        values.forEach(this::checkOperator);
        MultiValueIndexFieldFilter filter =
                new MultiValueIndexFieldFilter(this, values.stream().map(OpValue::getValue).collect(Collectors.toList()));
        if (filter.allValid()) {
            return noOpFilter();
        }
        return filter;
    }

    @Override
    protected IndexFieldFilter getSingleValueIndexFilter(OpValue<T> opValue) {
        checkOperator(opValue);
        return new SingleValueIndexFieldFilter(this, opValue.getValue());
    }

    protected void checkOperator(OpValue<T> opValue) {
        if (StringUtils.isNotEmpty(opValue.getOp()) && !opValue.getOp().equals("=") && !opValue.getOp().equals("==")) {
            throw new UnsupportedOperationException("Unsupported operator '" + opValue.getOp() + "'");
        }
    }

    private static class BasicCodec<T> implements IndexCodec<T> {
        private final T[] values;

        BasicCodec(T[] values) {
            this.values = values;
        }

        @Override
        public int encode(T value) {
            for (int i = 0, valuesLength = values.length; i < valuesLength; i++) {
                T t = values[i];
                if (t.equals(value)) {
                    return i;
                }
            }
            throw new IllegalArgumentException("Unknown value '" + value + "'");
        }

        @Override
        public T decode(int code) {
            return values[code];
        }

        @Override
        public boolean ambiguous(int code) {
            return false;
        }
    }

    private static class BasicCodecWithNa<T> implements IndexCodec<T> {
        private final T[] values;

        BasicCodecWithNa(T[] values) {
            this.values = values;
        }

        @Override
        public int encode(T value) {
            for (int i = 0, valuesLength = values.length; i < valuesLength; i++) {
                T t = values[i];
                if (t.equals(value)) {
                    // Add one, as 0 means "NA"
                    return i + 1;
                }
            }
            return NA;
        }

        @Override
        public T decode(int code) {
            if (code == NA) {
                return null;
            } else {
                // Subtract 1, as 0 is the special case for NA
                return values[code - 1];
            }
        }

        @Override
        public boolean ambiguous(int code) {
            return code == NA;
        }
    }

    @Override
    public int encode(T value) {
        return codec.encode(value);
    }

    @Override
    public T decode(int code) {
        return codec.decode(code);
    }

    @Override
    public boolean ambiguous(int code) {
        return codec.ambiguous(code);
    }

}
