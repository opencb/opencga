package org.opencb.opencga.storage.core.io.bit;

import java.io.ByteArrayOutputStream;

/**
 * Serialize streams of bits in a byte array.
 *
 * <code>
 *   os.write(ABC)
 *   os.write(123)
 *   os.write(xyz)
 *   os.write(4567)
 *
 *   os.toByteArray() => [yz123ABC,
 *                        0004567x]
 * </code>
 *
 * @see BitInputStream
 */
public class BitOutputStream {

    private final ByteArrayOutputStream os;
    private int buffer = 0; // only one byte buffer. Use int to avoid casts.
    private byte bufferCapacity = Byte.SIZE;

    public BitOutputStream() {
        this(100);
    }

    public BitOutputStream(int size) {
        os = new ByteArrayOutputStream(size);
    }

    /**
     * Write a value into the stream.
     *
     * @param value Up to 32 bits to write.
     * @param bitsLength Number of bits to write from the integer.
     */
    public void write(int value, int bitsLength) {
        int offset = 0;
        while (bufferCapacity <= bitsLength - offset) {
            // Complete buffer
            buffer |= (value >>> offset & mask(bufferCapacity)) << (Byte.SIZE - bufferCapacity);
            os.write(buffer);
            buffer = 0;
            offset += bufferCapacity;
            bufferCapacity = Byte.SIZE;
        }
        if (bitsLength > offset) {
            buffer |= ((value >>> offset) & mask(bitsLength - offset)) << (Byte.SIZE - bufferCapacity);
            bufferCapacity -= bitsLength - offset;
        }
    }

//    public void write(BitBuffer bitBuffer) {
//        //TODO
//    }

    public byte[] toByteArray() {
        if (bufferCapacity != Byte.SIZE) {
            os.write(buffer);
        }
        return os.toByteArray();
    }

    public void reset() {
        os.reset();
        buffer = 0;
        bufferCapacity = Byte.SIZE;
    }

    private static int mask(int i) {
        return (1 << i) - 1;
    }

    public BitBuffer toBitBuffer() {
        int extraBitLength = 0;
        if (bufferCapacity != Byte.SIZE) {
            os.write(buffer);
            extraBitLength = bufferCapacity;
        }
        byte[] bytes = os.toByteArray();
        return new BitBuffer(bytes, 0, (bytes.length) * Byte.SIZE - extraBitLength);
    }
}
