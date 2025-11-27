package org.opencb.opencga.storage.core.variant.index.core;

import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.tools.commons.BiConverter;
import org.opencb.opencga.core.config.storage.FieldConfiguration;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Context-less Data field.
 *
 * @param <T> Value type
 */
public abstract class DataField<T> extends DataFieldBase<T> {

    public DataField(FieldConfiguration configuration) {
        super(configuration);
    }

    public T readAndDecode(ByteBuffer bb) {
        return decode(read(bb));
    }

    public abstract T decode(ByteBuffer code);

    public <R> DataField<R> from(Function<R, T> converter, Function<T, R> deconverter) {
        return from(new BiConverter<R, T>() {
            @Override
            public T to(R r) {
                return converter.apply(r);
            }

            @Override
            public R from(T t) {
                return deconverter.apply(t);
            }
        });
    }

    public <R> DataField<R> from(BiConverter<R, T> converter) {
        R defaultValue = converter.from(DataField.this.getDefault());
        return new DataField<R>(DataField.this.getConfiguration()) {

            @Override
            public void move(ByteBuffer bb) {
                DataField.this.move(bb);
            }

            @Override
            public ByteBuffer read(ByteBuffer bb) {
                return DataField.this.read(bb);
            }

            @Override
            public int getByteLength(R value) {
                return DataField.this.getByteLength(converter.to(value));
            }

            @Override
            public void write(R value, ByteBuffer buffer) {
                DataField.this.write(converter.to(value), buffer);
            }

            @Override
            public void write(R value, ByteArrayOutputStream stream) {
                DataField.this.write(converter.to(value), stream);
            }

            @Override
            public R getDefault() {
                return defaultValue;
            }

            @Override
            public ByteBuffer encode(R value) {
                return DataField.this.encode(converter.to(value));
            }

            @Override
            public R decode(ByteBuffer code) {
                return converter.from(DataField.this.decode(code));
            }
        };
    }

    public <R, C> DataFieldWithContext<C, R> fromWithContext(BiConverter<Pair<C, R>, Pair<C, T>> converter) {
        Pair<C, R> defaultValue = converter.from(null);
        return new DataFieldWithContext<C, R>(DataField.this.getConfiguration()) {

            @Override
            public void move(ByteBuffer bb) {
                DataField.this.move(bb);
            }

            @Override
            public ByteBuffer read(ByteBuffer bb) {
                return DataField.this.read(bb);
            }

            @Override
            public int getByteLength(Pair<C, R> value) {
                return DataField.this.getByteLength(converter.to(value).getValue());
            }

            @Override
            public void write(Pair<C, R> value, ByteBuffer buffer) {
                DataField.this.write(converter.to(value).getValue(), buffer);
            }

            @Override
            public void write(Pair<C, R> value, ByteArrayOutputStream stream) {
                DataField.this.write(converter.to(value).getValue(), stream);
            }

            @Override
            public Pair<C, R> getDefault() {
                return defaultValue;
            }

            @Override
            public ByteBuffer encode(Pair<C, R> value) {
                return DataField.this.encode(converter.to(value).getValue());
            }

            @Override
            public R decode(C c, ByteBuffer code) {
                return converter.from(Pair.of(c, DataField.this.decode(code))).getValue();
            }

            @Override
            public R readAndDecode(C c, ByteBuffer code) {
                T decode = DataField.this.readAndDecode(code);
                return converter.from(Pair.of(c, decode)).getValue();
            }
        };
    }


}
