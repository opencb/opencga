package org.opencb.opencga.storage.core.variant.index.core;

import org.opencb.opencga.core.config.storage.FieldConfiguration;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * Variable width signed int data field.
 * https://protobuf.dev/programming-guides/encoding/#signed-ints
 */
public class VarSIntDataField extends VarIntDataField {

    public VarSIntDataField(FieldConfiguration configuration) {
        super(configuration);
    }

    @Override
    public Integer readAndDecode(ByteBuffer bb) {
        return decodeSign(super.readAndDecode(bb));
    }

    @Override
    public int getByteLength(Integer value) {
        return super.getByteLength(encodeSign(value));
    }

    @Override
    public void write(Integer value, ByteBuffer buffer) {
        super.write(encodeSign(value), buffer);
    }

    @Override
    public void write(Integer value, ByteArrayOutputStream stream) {
        super.write(encodeSign(value), stream);
    }

    @Override
    public ByteBuffer encode(Integer value) {
        return super.encode(encodeSign(value));
    }

    @Override
    public Integer decode(ByteBuffer code) {
        return decodeSign(super.decode(code));
    }

    public static Integer encodeSign(Integer value) {
        if (value == null) {
            return null;
        }
        if (value < 0) {
            value = ((-value) << 1) + 1;
        } else {
            value = (value << 1);
        }
        return value;
    }

    public static Integer decodeSign(Integer value) {
        if (value == null) {
            return null;
        }
        if ((value & 1) == 1) {
            value = value >> 1;
            value = -value;
        } else {
            value = value >> 1;
        }
        return value;
    }
}
