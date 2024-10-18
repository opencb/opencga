package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.FieldConfiguration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Varchar data field.
 * Read until the FIELD_SEPARATOR.
 */
public class VarCharDataField extends VariableWidthDataField<String> {

    protected static final byte FIELD_SEPARATOR = (byte) 0;

    public VarCharDataField(FieldConfiguration configuration) {
        super(configuration);
    }

    @Override
    public void move(ByteBuffer bb) {
        while (bb.hasRemaining() && bb.get() != FIELD_SEPARATOR) {
            continue;
        }
    }

    @Override
    public ByteBuffer read(ByteBuffer bb) {
        bb.mark();
        int length = 0;
        while (bb.hasRemaining() && bb.get() != FIELD_SEPARATOR) {
            length++;
        }
        bb.reset();
        ByteBuffer read = (ByteBuffer) bb.slice().limit(length);
        // move buffer
        bb.position(bb.position() + length);
        if (bb.hasRemaining()) {
            bb.get(); // skip separator
        }
        return read;
    }

    @Override
    public int getByteLength(String value) {
        int length = value == null ? 0 : value.length();
        // +1 for the FIELD_SEPARATOR
        return length + 1;
    }

    @Override
    public void write(String value, ByteBuffer buffer) {
        if (value != null) {
            buffer.put(value.getBytes(StandardCharsets.UTF_8));
        }
        buffer.put(FIELD_SEPARATOR);
    }

    @Override
    public void write(String value, ByteArrayOutputStream stream) {
        try {
            if (value != null) {
                stream.write(value.getBytes(StandardCharsets.UTF_8));
            }
            stream.write(FIELD_SEPARATOR);
        } catch (IOException e) {
            // This should never happen
            throw new UncheckedIOException(e);
        }
    }

    public boolean isDefault(ByteBuffer buffer) {
        return buffer.get(buffer.position() + 1) == FIELD_SEPARATOR;
    }

    @Override
    public String getDefault() {
        return "";
    }

    @Override
    public ByteBuffer encode(String value) {
        if (value == null) {
            return ByteBuffer.allocate(0);
        }
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String decode(ByteBuffer code) {
        if (code.isReadOnly()) {
            int limit = code.limit();
            byte[] bytes = new byte[limit];
            code.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        } else {
            return new String(code.array(), code.arrayOffset(), code.limit(), StandardCharsets.UTF_8);
        }
    }
}
