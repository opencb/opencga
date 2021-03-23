package org.opencb.opencga.storage.core.io.bit;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BitBufferTest {


    @Test
    public void testRW() {
        BitBuffer bitBuffer = new BitBuffer(300);
        runTest(bitBuffer, (byte) 'a', (byte) 'g', (byte) 'z', (byte) 0b10101, (byte) 0b11001,
                0b01111010101011010110100010100101, 0b01011111111101010011110100100011);
        runTest(bitBuffer, (byte) 't', (byte) 'a', (byte) 'w', (byte) 0b01010, (byte) 0b01010,
                0b11010101110110010011110111101100, 0b11010101110110010011110111101100);
    }

    protected void runTest(BitBuffer bitBuffer, byte c1, byte c2, byte c3, byte c4, byte c5, int i1, int i2) {
        bitBuffer.setByte(c1, 2);
        bitBuffer.setByte(c2, 13);
        bitBuffer.setByte(c3, 24);
        bitBuffer.setBytePartial(c4, 32, 5);
        bitBuffer.setBytePartial(c5, 38, 5);
        bitBuffer.setInt(i1, 100);
        bitBuffer.setInt(i2, 140);
        bitBuffer.setIntPartial(i1, 200, 13);
        bitBuffer.setIntPartial(i2, 214, 14);


        System.out.println("-----");
        System.out.println("c1 = " + c1 + " = " + byteToString(c1));
        System.out.println("c2 = " + c2 + " = " + byteToString(c2));
        System.out.println("c3 = " + c3 + " = " + byteToString(c3));
        System.out.println("c4 = " + c4 + " = " + byteToString(c4));
        System.out.println("c5 = " + c5 + " = " + byteToString(c5));
        System.out.println("-----");


        byte[] buffer = bitBuffer.getBuffer();
        for (int i = 0; i < buffer.length; i++) {
            byte b = buffer[i];
            System.out.println(i * Byte.SIZE + " = " + byteToString(b));
        }
        assertEquals(c1, bitBuffer.getByte(2));
        assertEquals(c2, bitBuffer.getByte(13));
        assertEquals(c3, bitBuffer.getByte(24));
        assertEquals(c4, bitBuffer.getBytePartial(32, 5));
        assertEquals(c5, bitBuffer.getBytePartial(38, 5));

        bitBuffer.setInt(i1, 100);
        bitBuffer.setInt(i2, 140);
        bitBuffer.setIntPartial(i1, 200, 13);
        bitBuffer.setIntPartial(i2, 214, 14);
        assertEquals(i1, bitBuffer.getInt(100));
        assertEquals(i2, bitBuffer.getInt(140));
        assertEquals(i1 & BitBuffer.mask(13), bitBuffer.getIntPartial(200, 13));
        assertEquals(i2 & BitBuffer.mask(14), bitBuffer.getIntPartial(214, 14));

        // Only 14 bits should be written. Reading more should return zeroes.
        assertEquals(i2 & BitBuffer.mask(14), bitBuffer.getInt(214));
    }


    public static String byteToString(byte b) {
        return binaryToString(b, Byte.SIZE);
    }

    public static String shortToString(short s) {
        return binaryToString(s, Short.SIZE);
    }

    protected static String binaryToString(int number, int i) {
        String str = Integer.toBinaryString(number);
        if (str.length() > i) {
            str = str.substring(str.length() - i);
        }
        return StringUtils.leftPad(str, i, '0');
    }
}