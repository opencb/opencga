package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.apache.commons.lang3.tuple.Pair;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * Data field that, for encode and decode, requires a context C.
 *
 * @param <C> Context used to encode and decode the value
 * @param <T> Value type
 */
public abstract class DataFieldWithContext<C, T> extends DataFieldBase<Pair<C, T>> {
    public DataFieldWithContext(IndexFieldConfiguration configuration) {
        super(configuration);
    }

    public ByteBuffer write(C context, T value) {
        return write(Pair.of(context, value));
    }

    public int getByteLength(C context, T value) {
        return getByteLength(Pair.of(context, value));
    }

    public void write(C context, T value, ByteBuffer buffer) {
        write(Pair.of(context, value), buffer);
    }

    public void write(C context, T value, ByteArrayOutputStream stream) {
        write(Pair.of(context, value), stream);
    }

    public ByteBuffer encode(C context, T value) {
        return encode(Pair.of(context, value));
    }

    public abstract T decode(C c, ByteBuffer code);

    public abstract T readAndDecode(C c, ByteBuffer code);
}
