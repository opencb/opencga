package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.core.config.storage.FieldConfiguration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

public class IntegerDataField extends DataField<Integer> {

    public IntegerDataField(FieldConfiguration configuration) {
        super(configuration);
    }

    @Override
    public void move(ByteBuffer bb) {
        bb.position(bb.position() + Integer.BYTES);
    }

    @Override
    public ByteBuffer read(ByteBuffer bb) {
        ByteBuffer read = bb.slice();
        read.limit(Integer.BYTES);
        move(bb);
        return read;
    }

    @Override
    public Integer readAndDecode(ByteBuffer bb) {
        return bb.getInt();
    }

    @Override
    public int getByteLength(Integer value) {
        return Integer.BYTES;
    }

    @Override
    public void write(Integer value, ByteBuffer buffer) {
        buffer.putInt(value);
    }

    @Override
    public void write(Integer value, ByteArrayOutputStream stream) {
        try {
            stream.write(Bytes.toBytes(value));
        } catch (IOException e) {
            // This should never happen
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Integer getDefault() {
        return 0;
    }

    @Override
    public ByteBuffer encode(Integer value) {
        return ByteBuffer.allocate(4).putInt(value);
    }

    @Override
    public Integer decode(ByteBuffer code) {
        return code.getInt();
    }
}
