package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Field of the DataSchema.
 * Similar to {@link IndexField}, but for the DataSchema.
 * This field does not allow filters.
 * <p>
 *     This class is used to read and write the data of the DataSchema.
 *     The ByteBuffer contains a set of entries, each entry contains a set of fields.
 * @param <T>
 */
public abstract class DataFieldBase<T> {

    private final IndexFieldConfiguration configuration;

    public DataFieldBase(IndexFieldConfiguration configuration) {
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

    /**
     * Read the next value from the ByteBuffer.
     * The ByteBuffer position will be moved to the next field.
     *
     * @param bb ByteBuffer
     * @return   ByteBuffer with the read value
     */
    public abstract ByteBuffer read(ByteBuffer bb);

    public ByteBuffer write(T value) {
        ByteBuffer buffer = ByteBuffer.allocate(getByteLength(value));
        write(value, buffer);
        return buffer;
    }

    public abstract int getByteLength(T value);

    public abstract void write(T value, ByteBuffer buffer);

    public abstract void write(T value, ByteArrayOutputStream stream);

    public boolean isDefault(T value) {
        return Objects.equals(value, getDefault());
    }

    public abstract T getDefault();

    public ByteBuffer getDefaultEncoded() {
        return encode(getDefault());
    }

    public abstract ByteBuffer encode(T value);

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DataField{");
        sb.append("configuration=").append(configuration);
        sb.append('}');
        return sb.toString();
    }
}
