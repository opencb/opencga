package org.opencb.opencga.storage.core.io.bit;

import org.apache.commons.lang3.StringUtils;

public class BitBuffer {

    private final byte[] buffer;
    private final int bitOffset;
    private final int bitLength;

    public BitBuffer(BitBuffer copy) {
        if (copy.getBitOffset() == 0) {
            bitOffset = copy.bitOffset;
            bitLength = copy.bitLength;
            int bytes = bitsToBytes(copy.bitLength);
            buffer = new byte[bytes];
            System.arraycopy(copy.buffer, 0, buffer, 0, bytes);
        } else {
            bitOffset = copy.bitOffset % Byte.SIZE;
            bitLength = copy.bitLength;
            int extraBytesOffset = (copy.bitOffset - bitOffset) / Byte.SIZE;
            int bytesLength = bitsToBytes(copy.bitLength + bitOffset);
            buffer = new byte[bytesLength];
            System.arraycopy(copy.buffer, extraBytesOffset, buffer, 0, buffer.length);
        }
    }

    public BitBuffer(int bitLength) {
        this(new byte[bitsToBytes(bitLength)], 0, bitLength);
    }

    public BitBuffer(byte[] buffer) {
        this.buffer = buffer;
        this.bitOffset = 0;
        this.bitLength = buffer.length * Byte.SIZE;
    }

    protected BitBuffer(byte[] buffer, int bitsOffset, int bitsLength) {
        this.buffer = buffer;
        this.bitOffset = bitsOffset;
        this.bitLength = bitsLength;
        if (bitsToBytes(bitsOffset + bitsLength) > buffer.length) {
            throw new IllegalArgumentException("byte[] buffer too small."
                    + " offset : " + bitsOffset + " bits " + bitsToBytes(bitsOffset) + " bytes ,"
                    + " length : " + bitsLength + " bits " + bitsToBytes(bitsLength) + " bytes ,"
                    + " actual buffer bytes length : " + buffer.length + " bytes");
        }
    }

    public static int bitsToBytes(int bitsLength) {
        return (int) Math.ceil(bitsLength / ((float) Byte.SIZE));
    }

    public BitBuffer getBitBuffer(int bitOffset, int bitLength) {
        if ((bitOffset + bitLength) > getBitLength()) {
            throw new IndexOutOfBoundsException("Bit offset request: " + bitOffset
                    + ", bit length request: " + bitLength
                    + ", actual bit length: " + this.bitLength);
        }
        return new BitBuffer(getBuffer(), getBitOffset() + bitOffset, bitLength);
    }

    public int getInt(int bitOffset) {
        return getIntPartial(bitOffset, Integer.SIZE);
    }

    public int getIntPartial(int bitOffset, int bitLength) {
        if (bitLength > Integer.SIZE) {
            throw new IllegalArgumentException("Reading " + bitLength + " bits. Expecting read a partial int, up to 32 bits");
        }
        int r = 0;
        int bytes = bitLength / Byte.SIZE;
        int bits = bitLength % Byte.SIZE;
        for (int i = 0; i < bitLength - bits; i += 8) {
            r |= (Byte.toUnsignedInt(getByte(bitOffset + i))) << i;
        }
        if (bits != 0) {
            r |= (Byte.toUnsignedInt(getBytePartial(bitOffset + bytes * Byte.SIZE, bits)) << (bytes * Byte.SIZE));
        }
        return r;
    }

    public byte getByte(int bitOffset) {
        return getBytePartial(bitOffset, Byte.SIZE);
    }

    public byte getBytePartial(int bitOffset, int length) {
        if ((bitOffset + length) > this.bitLength) {
            throw new IndexOutOfBoundsException("Bit offset request: " + bitOffset
                    + ", bit length request: " + length
                    + ", actual bit length: " + this.bitLength);
        }
        if (length > Byte.SIZE) {
            throw new IllegalArgumentException("Reading " + length + " bits. Expecting read a partial byte");
        }
        if (length == 0) {
            return 0;
        }
        int totalBitOffset = bitOffset + this.bitOffset;
        int byteOffset = totalBitOffset / Byte.SIZE;
        int partialBitOffset = totalBitOffset % Byte.SIZE;

        int r;
        if (length > (Byte.SIZE - partialBitOffset)) {
            r = ((buffer[byteOffset] >>> partialBitOffset) & mask(Byte.SIZE - partialBitOffset));
            length -= Byte.SIZE - partialBitOffset;
            r |= (buffer[byteOffset + 1] & mask(length)) << (Byte.SIZE - partialBitOffset);
        } else {
            r = ((buffer[byteOffset] >>> partialBitOffset) & mask(length));
        }
        return (byte) r;
    }


    public void setInt(int value, int bitOffset) {
        setIntPartial(value, bitOffset, Integer.SIZE);
    }

    public void setIntPartial(int value, int bitOffset, int bitLength) {
        int bytes = bitLength / Byte.SIZE;
        int bits = bitLength % Byte.SIZE;

        int i;
        for (i = 0; i < (bitLength - bits); i += 8) {
            setByte((byte) (value >> i), bitOffset + i);
        }
        setBytePartial((byte) (value >> i), bitOffset + i, bits);
    }

    public void setByte(byte value) {
        setByte(value, 0);
    }

    public void setByte(byte value, int bitOffset) {
        setBytePartial(value, bitOffset, Byte.SIZE);
    }

    public void setBitBuffer(BitBuffer value, int bitOffset) {
        if (value.getBitLength() + bitOffset > this.bitLength) {
            throw new IndexOutOfBoundsException("Bit offset request: " + bitOffset
                    + ", bit length request: " + value.getBitLength()
                    + ", actual bit length: " + this.bitLength);
        }
        // TODO: Speed up with System.arrayCopy if possible
        int bytes = value.getBitLength() / 8;
        int bits = value.getBitLength() % 8;
        for (int i = 0; i < bytes; i++) {
            byte aByte = value.getByte(i * Byte.SIZE);
            setByte(aByte, i * Byte.SIZE + bitOffset);
        }
        if (bits != 0) {
            setBytePartial(value.getBytePartial(bytes * Byte.SIZE, bits), bytes * Byte.SIZE + bitOffset, bits);
        }
    }

    public void setBytePartial(byte value, int bitOffset, int length) {
        if ((bitOffset + length) > bitLength) {
            throw new IndexOutOfBoundsException("Bit offset request: " + bitOffset
                    + ", bit length request: " + length
                    + ", actual bit length: " + this.bitLength);
        }
        if (length > Byte.SIZE) {
            throw new IllegalArgumentException("Reading " + length + " bits. Expecting read a partial byte");
        }
        if (length == 0) {
            return;
        }

        int totalBitOffset = bitOffset + this.bitOffset;
        int byteOffset = totalBitOffset / Byte.SIZE;
        int partialBitOffset = totalBitOffset % Byte.SIZE;

        if (length <= (Byte.SIZE - partialBitOffset)) {
            // Clear
            buffer[byteOffset] &= ~(mask(length) << partialBitOffset);
            // Set
            buffer[byteOffset] |= (value & mask(length)) << partialBitOffset;
        } else {
            // Clear
            buffer[byteOffset]     &= mask(partialBitOffset);
            buffer[byteOffset + 1] &= ~(mask(length - (Byte.SIZE - partialBitOffset)));

            // Set
            buffer[byteOffset] |= value << partialBitOffset;
            buffer[byteOffset + 1] |= (value >>> (Byte.SIZE - partialBitOffset)) & mask(length - (Byte.SIZE - partialBitOffset));
        }
    }

    public static BitBuffer copy(byte[] buffer, int offset, int length) {
        // TODO: Smart copy
        byte[] copy = new byte[buffer.length];
        System.arraycopy(buffer, 0, copy, 0, buffer.length);
        return new BitBuffer(copy, offset, length);
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public int getBitLength() {
        return bitLength;
    }

    public int getByteLength() {
        return bitsToBytes(bitLength);
    }

    public int getBitOffset() {
        return bitOffset;
    }

    public int getByteOffset() {
        return bitOffset / Byte.SIZE;
    }

    public static int mask(int bits) {
        return (1 << bits) - 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BitBuffer that = (BitBuffer) o;

        return compareTo(that) == 0;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    public int compareTo(BitBuffer other) {
        int compare = Integer.compare(bitLength, other.bitLength);
        if (compare != 0) {
            return compare;
        } else {
            int partialBits = bitLength % Byte.SIZE;
            for (int i = 0; i < bitLength - partialBits; i += Byte.SIZE) {
                compare = Byte.compare(getByte(i), other.getByte(i));
                if (compare != 0) {
                    return compare;
                }
            }
            if (partialBits != 0) {
                return Byte.compare(getBytePartial(bitLength - partialBits, partialBits),
                        other.getBytePartial(bitLength - partialBits, partialBits));
            } else {
                return 0;
            }

        }
    }

    public void clear() {
        setBitBuffer(new BitBuffer(bitLength), 0);
    }

    @Override
    public String toString() {
        StringBuilder msg = new StringBuilder("BitBuffer{bitOffset:" + bitOffset + ", bitLength:" + bitLength + ", [");
        int lastBits = bitLength % Byte.SIZE;
        for (int i = 0; i < bitLength - lastBits; i += Byte.SIZE) {
            msg.append(byteToString(getByte(i))).append(", ");
        }
        if (lastBits != 0) {
            msg.append(binaryToString(getBytePartial(bitLength - lastBits, lastBits), lastBits));
        }
        msg.append("]}");
        return msg.toString();
    }


    private static String byteToString(byte b) {
        return binaryToString(b, Byte.SIZE);
    }

    private static String shortToString(short s) {
        return binaryToString(s, Short.SIZE);
    }

    private static String binaryToString(int number, int i) {
        String str = Integer.toBinaryString(number);
        if (str.length() > i) {
            str = str.substring(str.length() - i);
        }
        return StringUtils.leftPad(str, i, '0');
    }
}
