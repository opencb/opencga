package org.opencb.opencga.storage.core.io.bit;

import java.io.ByteArrayOutputStream;

public class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

    public ExposedByteArrayOutputStream(int size) {
        super(size);
    }

    public byte[] getBuffer() {
        return buf;
    }

    public int length() {
        return this.count;
    }

}
