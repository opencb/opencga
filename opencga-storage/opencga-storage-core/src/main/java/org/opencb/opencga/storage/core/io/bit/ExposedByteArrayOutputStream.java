package org.opencb.opencga.storage.core.io.bit;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

    public ExposedByteArrayOutputStream() {
        super();
    }

    public ExposedByteArrayOutputStream(int size) {
        super(size);
    }

    public byte[] getBuffer() {
        return buf;
    }

    public ByteBuffer toByteByffer() {
        return ByteBuffer.wrap(buf, 0, length());
    }

    public int length() {
        return this.count;
    }

}
