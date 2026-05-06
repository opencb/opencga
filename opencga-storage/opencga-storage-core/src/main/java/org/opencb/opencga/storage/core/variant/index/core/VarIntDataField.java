package org.opencb.opencga.storage.core.variant.index.core;

import org.opencb.opencga.core.config.storage.FieldConfiguration;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * Variable width int data field.
 * https://protobuf.dev/programming-guides/encoding/#varints
 */
public class VarIntDataField extends VariableWidthDataField<Integer> {

    private static final int VALUE_MASK = 0b0111_1111;
    private static final int CONTINUATION_BIT_MASK = 0b1000_0000;

    public VarIntDataField(FieldConfiguration configuration) {
        super(configuration);
    }

    @Override
    public void move(ByteBuffer bb) {
        bb.position(bb.position() + Integer.BYTES);
    }

    @Override
    public ByteBuffer read(ByteBuffer bb) {
        bb.mark();
        int length = 0;
        while (bb.hasRemaining()) {
            byte b = bb.get();
            length++;
            if ((b & CONTINUATION_BIT_MASK) == 0) {
                break;
            }
        }
        bb.reset();
        ByteBuffer read = (ByteBuffer) bb.slice().limit(length);
        // move buffer
        bb.position(bb.position() + length);
        return read;
    }

    @Override
    public Integer readAndDecode(ByteBuffer bb) {
        int result = 0;
        int shift = 0;
        while (bb.hasRemaining()) {
            byte b = bb.get();
            result |= (b & VALUE_MASK) << shift;
            if ((b & CONTINUATION_BIT_MASK) == 0) {
                return result;
            }
            shift += 7;
        }
        return result;
    }

    @Override
    public int getByteLength(Integer value) {
        if (value == null) {
            return 0;
        } else if (value < 0) {
            return 5;
        } else if (value < (1 << 7)) {
            return 1;
        } else if (value < (1 << 14)) {
            return 2;
        } else if (value < (1 << 21)) {
            return 3;
        } else if (value < (1 << 28)) {
            return 4;
        } else {
            return 5;
        }
    }

    @Override
    public void write(Integer value, ByteBuffer buffer) {
        // While "value" without the value_mask is not 0 (i.e. has more than 7 bits)
        while ((value & ~VALUE_MASK) != 0) {
            buffer.put((byte) ((value & VALUE_MASK) | CONTINUATION_BIT_MASK));
            value >>>= 7;
        }
        buffer.put((byte) (value & VALUE_MASK));
    }

    @Override
    public void write(Integer value, ByteArrayOutputStream stream) {
        // While "value" without the value_mask is not 0 (i.e. has more than 7 bits)
        while ((value & ~VALUE_MASK) != 0) {
            stream.write((value & VALUE_MASK) | CONTINUATION_BIT_MASK);
            value >>>= 7;
        }
        stream.write(value & VALUE_MASK);
    }

    @Override
    public Integer getDefault() {
        return 0;
    }

    @Override
    public ByteBuffer encode(Integer value) {
        ByteBuffer buffer = ByteBuffer.allocate(getByteLength(value));
        write(value, buffer);
        buffer.flip();
        return buffer;
    }

    @Override
    public Integer decode(ByteBuffer code) {
        int result = 0;
        for (int shift = 0; shift < Integer.SIZE; shift += 7) {
            byte b = code.get();
            result |= (b & VALUE_MASK) << shift;
            if ((b & CONTINUATION_BIT_MASK) == 0) {
                return result;
            }
        }
        return result;
    }
}
