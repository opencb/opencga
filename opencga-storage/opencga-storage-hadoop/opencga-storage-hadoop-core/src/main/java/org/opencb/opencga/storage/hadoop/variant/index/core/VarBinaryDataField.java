package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.FieldConfiguration;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class VarBinaryDataField extends VariableWidthDataField<ByteBuffer> {

    private final VarIntDataField lengthField;

    public VarBinaryDataField(FieldConfiguration configuration) {
        super(configuration);
        lengthField = new VarIntDataField(configuration);
    }

    @Override
    public ByteBuffer readAndDecode(ByteBuffer bb) {
        Integer length = lengthField.readAndDecode(bb);
        try {
            return (ByteBuffer) bb.slice().limit(length);
        } catch (Exception e) {
            System.out.println("length = " + length);
            System.out.println("bb.limit() = " + bb.limit());
            throw e;
        }
    }

    @Override
    public ByteBuffer read(ByteBuffer bb) {
        bb.mark();
        int start = bb.position();
        Integer length = lengthField.readAndDecode(bb);
        if (length == 0) {
            return ByteBuffer.wrap(new byte[0]);
        }
        int end = bb.position();
        int totalLength = length + (end - start);

        bb.rewind();
        ByteBuffer code = (ByteBuffer) bb.slice().limit(totalLength);
        // move buffer
        bb.position(bb.position() + totalLength);
        return code;
    }

    @Override
    public int getByteLength(ByteBuffer value) {
        return lengthField.getByteLength(value.limit()) + value.limit();
    }

    @Override
    public void write(ByteBuffer value, ByteBuffer buffer) {
        value.rewind();
        lengthField.write(value.limit(), buffer);
        buffer.put(value);
        value.rewind();
    }

    @Override
    public void write(ByteBuffer value, ByteArrayOutputStream stream) {
        value.rewind();
        lengthField.write(value.limit(), stream);
        for (int i = 0; i < value.limit(); i++) {
            stream.write(value.get(i));
        }
        value.rewind();
    }

    @Override
    public ByteBuffer getDefault() {
        return ByteBuffer.wrap(new byte[]{0});
    }

    @Override
    public ByteBuffer encode(ByteBuffer value) {
        value.rewind();
        ByteBuffer code = ByteBuffer.allocate(getByteLength(value));
        write(value, code);
        code.rewind();
        return code;
    }

    @Override
    public ByteBuffer decode(ByteBuffer code) {
        code.rewind();
        Integer length = lengthField.readAndDecode(code);
        if (length == 0) {
            return ByteBuffer.wrap(new byte[0]);
        }
        return (ByteBuffer) code.slice().limit(length);
    }
}
