package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.variant.query.OpValue;
import org.opencb.opencga.storage.core.variant.query.Values;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.AndIndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.NoOpIndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.OrIndexFieldFilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class IndexField<T> {

    private final IndexFieldConfiguration configuration;
    private final int bitOffset;

    public IndexField(IndexFieldConfiguration configuration, int bitOffset) {
        this.configuration = configuration;
        this.bitOffset = bitOffset;
    }

    public String getId() {
        return configuration.getId();
    }

    public IndexFieldConfiguration.Source getSource() {
        return configuration.getSource();
    }

    public String getKey() {
        return configuration.getKey();
    }

    public IndexFieldConfiguration getConfiguration() {
        return configuration;
    }

    public IndexFieldConfiguration.Type getType() {
        return configuration.getType();
    }

    public abstract int getBitLength();

    public int getBitOffset() {
        return bitOffset;
    }

    public void move(BitBuffer bb) {
        read(bb);
    }

    public int read(BitBuffer bb) {
        return bb.getIntPartial(getBitOffset(), getBitLength());
    }

    public T readAndDecode(BitBuffer bb) {
        return decode(read(bb));
    }

    public void write(T value, BitBuffer bitBuffer) {
        bitBuffer.setIntPartial(encode(value), getBitOffset(), getBitLength());
    }

    public IndexFieldFilter noOpFilter() {
        return new NoOpIndexFieldFilter(this);
    }

    public IndexFieldFilter buildFilter(VariantQueryUtils.QueryOperation operation, Collection<T> values) {
        return buildFilter(new Values<>(operation, values.stream().map(v -> new OpValue<>("=", v)).collect(Collectors.toList())));
    }

    public IndexFieldFilter buildFilter(OpValue<T> query) {
        return buildFilter(new Values<>(null, Collections.singletonList(query)));
    }

    public IndexFieldFilter buildFilter(Values<OpValue<T>> query) {
        if (query == null || query.isEmpty()) {
            return new NoOpIndexFieldFilter(this);
        } else if (query.getValues().size() == 1) {
            OpValue<T> opValue = query.getValues().get(0);
            return getSingleValueIndexFilter(opValue);
        } else {
            if (query.getOperation().equals(VariantQueryUtils.QueryOperation.OR)) {
                return getOrIndexFilter(query.getValues());
            } else {
                return getAndIndexFilter(query.getValues());
            }
        }
    }

    protected IndexFieldFilter getAndIndexFilter(List<OpValue<T>> values) {
        // TODO: Improve filter
        List<IndexFieldFilter> filters = new ArrayList<>(values.size());
        for (OpValue<T> value : values) {
            filters.add(getSingleValueIndexFilter(value));
        }
        return new AndIndexFieldFilter(this, filters);
    }

    protected IndexFieldFilter getOrIndexFilter(List<OpValue<T>> values) {
        // TODO: Improve filter
        List<IndexFieldFilter> filters = new ArrayList<>(values.size());
        for (OpValue<T> value : values) {
            filters.add(getSingleValueIndexFilter(value));
        }
        return new OrIndexFieldFilter(this, filters);
    }

    protected abstract IndexFieldFilter getSingleValueIndexFilter(OpValue<T> opValue);

    public abstract int encode(T value);

    public abstract T decode(int code);

    public <R> IndexField<R> from(Function<R, T> converter, Function<T, R> deconverter) {
        return new IndexField<R>(configuration, bitOffset) {
            @Override
            public int getBitLength() {
                return IndexField.this.getBitLength();
            }

            @Override
            protected IndexFieldFilter getSingleValueIndexFilter(OpValue<R> opValue) {
                return IndexField.this.getSingleValueIndexFilter(convertOpValue(opValue));
            }

            @Override
            protected IndexFieldFilter getAndIndexFilter(List<OpValue<R>> values) {
                return IndexField.this.getAndIndexFilter(values.stream().map(this::convertOpValue).collect(Collectors.toList()));
            }

            @Override
            protected IndexFieldFilter getOrIndexFilter(List<OpValue<R>> values) {
                return IndexField.this.getOrIndexFilter(values.stream().map(this::convertOpValue).collect(Collectors.toList()));
            }

            private OpValue<T> convertOpValue(OpValue<R> opValue) {
                return new OpValue<>(opValue.getOp(), converter.apply(opValue.getValue()));
            }

            @Override
            public int encode(R value) {
                T t = converter.apply(value);
                return IndexField.this.encode(t);
            }

            @Override
            public R decode(int code) {
                T v = IndexField.this.decode(code);
                return deconverter.apply(v);
            }
        };
    }

}
