package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.storage.core.variant.query.OpValue;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.MaskIndexFieldFilter;

import java.util.*;
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

    public static CategoricalMultiValuedIndexField<String> createMultiValued(IndexFieldConfiguration configuration, int bitOffset) {
        return new CategoricalMultiValuedIndexField<>(
                configuration,
                bitOffset,
                configuration.getValues(),
                configuration.getValuesMapping());
    }

    public CategoricalMultiValuedIndexField(IndexFieldConfiguration configuration, int bitOffset, T[] values) {
        this(configuration, bitOffset, values, (Map<T, List<T>>) null);
    }

    public CategoricalMultiValuedIndexField(IndexFieldConfiguration configuration, int bitOffset, T[] values,
                                            Map<T, List<T>> valuesMapping) {
        this(configuration, bitOffset, values, new MaskValueCodec<>(values, valuesMapping, configuration.getNullable()));
    }

    private CategoricalMultiValuedIndexField(IndexFieldConfiguration configuration, int bitOffset, T[] values, MaskValueCodec<T> codec) {
        super(configuration, bitOffset, values.length, codec);
        numBits = codec.numBits;
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
        public static final int NA = 0;
        private final T[] values;
        private final Integer otherValuePosition;
        private final Map<T, Integer> valuesPosition;
        private final int numBits;
        private final int ambiguousValues;

        MaskValueCodec(T[] values, Map<T, List<T>> valuesMapping, boolean withOther) {
            if (withOther) {
                numBits = values.length + 1;
                this.otherValuePosition = values.length;
                this.values = Arrays.copyOf(values, values.length + 1);
                this.values[otherValuePosition] = null;
            } else {
                this.values = values;
                numBits = values.length;
                otherValuePosition = null;
            }
            if (valuesMapping == null) {
                valuesMapping = Collections.emptyMap();
            }
            valuesPosition = new HashMap<>();
            int ambiguousValues = 0;
            for (int pos = 0; pos < values.length; pos++) {
                T value = values[pos];
                valuesPosition.put(value, pos);
                List<T> alias = valuesMapping.getOrDefault(value, Collections.emptyList());
                if (alias.size() > 1 || alias.size() == 1 && alias.get(0).equals(value)) {
                    // Is ambiguous
                    ambiguousValues |= 1 << pos;
                    for (T valueAlias : alias) {
                        valuesPosition.put(valueAlias, pos);
                    }
                }
            }
            if (otherValuePosition != null) {
                ambiguousValues |= 1 << otherValuePosition;
            }
            this.ambiguousValues = ambiguousValues;
            if (values.length > Integer.SIZE) {
                throw new IllegalArgumentException("Unable to represent more than " + Integer.SIZE
                        + " values in a " + CategoricalMultiValuedIndexField.class.getSimpleName());
            }
        }

        @Override
        public int encode(List<T> valuesList) {
            int code = 0;
            for (T value : valuesList) {
                Integer pos = valuesPosition.getOrDefault(value, otherValuePosition);
                if (pos != null) {
                    code |= 1 << pos;
                }
            }
            return code;
        }

        @Override
        public List<T> decode(int code) {
            if (code == 0) {
                return Collections.emptyList();
            } else {
                List<T> decode = new ArrayList<>(numBits);
                for (int i = 0; i < numBits; i++) {
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
            return code == NA || (code & ambiguousValues) != 0;
        }
    }

}
