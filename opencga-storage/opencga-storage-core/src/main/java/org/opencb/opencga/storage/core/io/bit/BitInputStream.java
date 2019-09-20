package org.opencb.opencga.storage.core.io.bit;

/**
 * Read bits from a byte array.
 *
 * <code>
 *   is = new BitInputStream([yz123ABC,
 *                            0004567x])
 *
 *   is.read(3) => ABC
 *   is.read(3) => 123
 *   is.read(3) => xyz
 *   is.read(4) => 4567
 * </code>
 *
 * @see BitOutputStream
 */
public class BitInputStream {

    private final byte[] value;
    private final int numBits;
    private int remainingBits;
    private int idx;
    private int bitIdx;

    public BitInputStream(byte[] value) {
        this(value, 0, value.length);
    }

    public BitInputStream(byte[] value, int offset, int length) {
        this.value = value;
        numBits = length * Byte.SIZE;
        remainingBits = numBits;
        idx = offset;
        bitIdx = 0;
    }

    public int remainingBits() {
        return remainingBits;
    }

    public byte readByte() {
        return readByte(Byte.SIZE);
    }

    /**
     * Read up to 8 bits (one byte).
     *
     * @param length Number of bits to read.
     * @return read value
     */
    public byte readByte(int length) {
        if (length > Byte.SIZE) {
            throw new IllegalArgumentException();
        }
        remainingBits -= length;
        if (remainingBits < 0) {
            throw new IllegalArgumentException();
        }
        int r; // Use int to avoid intermediate castings
        if (length > (Byte.SIZE - bitIdx)) {
            r = ((value[idx] >>> bitIdx) & mask(Byte.SIZE - bitIdx));
            idx++;
            length -= Byte.SIZE - bitIdx;
            r |= (value[idx] & mask(length)) << (Byte.SIZE - bitIdx);
            bitIdx = length;
        } else {
            r = ((value[idx] >>> bitIdx) & mask(length));
            bitIdx += length;
            if (bitIdx == Byte.SIZE) {
                bitIdx = 0;
                idx++;
            }
        }
        return (byte) r;
    }

    public byte[] read(int length) {
        int bytes = length / Byte.SIZE;
        int bits = length % Byte.SIZE;
        byte[] result = new byte[bytes + (bits > 0 ? 1 : 0)];
        for (int i = 0; i < bytes; i++) {
            result[i] = readByte();
        }
        if (bits > 0) {
            result[bytes] = readByte(bits);
        }
        return result;
    }

    private static int mask(int i) {
        return (1 << i) - 1;
    }

}
