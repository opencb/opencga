package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Field of the DataSchema.
 * Similar to {@link IndexField}, but for the DataSchema.
 * This field does not allow filters.
 * <p>
 *     This class is used to read and write the data of the DataSchema.
 *     The ByteBuffer contains a set of entries, each entry contains a set of fields.
 * @param <T>
 */
public abstract class DataField<T> {

    private final IndexFieldConfiguration configuration;

    public DataField(IndexFieldConfiguration configuration) {
        this.configuration = configuration;
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

    public void move(ByteBuffer bb) {
        read(bb);
    }

    public abstract ByteBuffer read(ByteBuffer bb);

    public T readAndDecode(ByteBuffer bb) {
        return decode(read(bb));
    }

    public ByteBuffer write(T value) {
        ByteBuffer buffer = ByteBuffer.allocate(getByteLength(value));
        write(value, buffer);
        return buffer;
    }

    public abstract int getByteLength(T value);

    public abstract void write(T value, ByteBuffer buffer);

    public abstract void write(T value, ByteArrayOutputStream stream);

    public abstract ByteBuffer encode(T value);

    public abstract T decode(ByteBuffer code);

    public <R> DataField<R> from(Function<R, T> converter, Function<T, R> deconverter) {
        return new DataField<R>(configuration) {

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
                return DataField.this.getByteLength(converter.apply(value));
            }

            @Override
            public void write(R value, ByteBuffer buffer) {
                DataField.this.write(converter.apply(value), buffer);
            }

            @Override
            public void write(R value, ByteArrayOutputStream stream) {
                DataField.this.write(converter.apply(value), stream);
            }

            @Override
            public ByteBuffer encode(R value) {
                return DataField.this.encode(converter.apply(value));
            }

            @Override
            public R decode(ByteBuffer code) {
                return deconverter.apply(DataField.this.decode(code));
            }
        };
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DataField{");
        sb.append("configuration=").append(configuration);
        sb.append('}');
        return sb.toString();
    }
}
