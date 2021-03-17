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
public class BitInputStream extends BitBuffer {

    private int bitsAvailable;
    private int bitsRead;

    public BitInputStream(byte[] value) {
        this(value, 0, value.length);
    }

    public BitInputStream(byte[] value, int offset, int length) {
        super(value, offset * Byte.SIZE, length * Byte.SIZE);
        bitsAvailable = length * Byte.SIZE;
        bitsRead = 0;
    }

    public BitInputStream(BitBuffer bitBuffer) {
        super(bitBuffer.getBuffer(), bitBuffer.getBitOffset(), bitBuffer.getBitLength());
        bitsAvailable = bitBuffer.getBitLength();
        bitsRead = 0;
    }

    public int remainingBits() {
        return bitsAvailable;
    }

    public byte readByte() {
        return readBytePartial(Byte.SIZE);
    }

    public BitBuffer readBitBuffer(int bitLength) {
        BitBuffer bitBuffer = getBitBuffer(bitsRead, bitLength);
        bitsRead += bitLength;
        bitsAvailable -= bitLength;
        return bitBuffer;
    }

    /**
     * Read up to 32 bits (one int).
     *
     * @param length Number of bits to read.
     * @return read value
     */
    public int readIntPartial(int length) {
        int r = getIntPartial(bitsRead, length);
        bitsRead += length;
        bitsAvailable -= length;
        return r;
    }

    /**
     * Read up to 8 bits (one byte).
     *
     * @param length Number of bits to read.
     * @return read value
     */
    public byte readBytePartial(int length) {
        byte b = super.getBytePartial(bitsRead, length);
        bitsRead += length;
        bitsAvailable -= length;
        return b;
    }

    public byte[] readBytes(int numValues, int valueBitLength) {
        byte[] bytes = new byte[numValues];
        for (int i = 0; i < numValues; i++) {
            bytes[i] = readBytePartial(valueBitLength);
        }
        return bytes;
    }

    public byte[] read(int lengthBits) {
        int bytes = lengthBits / Byte.SIZE;
        int bits = lengthBits % Byte.SIZE;
        byte[] result = new byte[bytes + (bits > 0 ? 1 : 0)];
        for (int i = 0; i < bytes; i++) {
            result[i] = readByte();
        }
        if (bits > 0) {
            result[bytes] = readBytePartial(bits);
        }
        return result;
    }

    /**
     * Skips over and discards n bits of data from this input stream.
     *
     * @param numBits n – the number of bits to be skipped.
     * @return the actual number of bytes skipped.
     */
    public long skip(int numBits) {
        int skipped;
        if (bitsAvailable < numBits) {
            skipped = this.bitsAvailable;
        } else {
            skipped = numBits;
        }
        bitsRead += skipped;
        bitsAvailable -= skipped;
        return skipped;
    }

}
