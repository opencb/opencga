package org.opencb.opencga.storage.core.io.bit;

import org.apache.commons.lang3.RandomUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class BitStreamTest {

    public static final int LENGTH = 1000;
    private byte[] bits;
    private byte[] bytes;

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private BitInputStream inputStream;

    @Before
    public void setUp() throws Exception {
//        Random random = new Random(2);
        bits = new byte[LENGTH * Byte.SIZE];
        bytes = new byte[LENGTH];

        for (int i = 0; i < bits.length; i++) {
            bits[i] = (byte) (RandomUtils.nextBoolean() ? 1 : 0);
        }
        for (int i = 0; i < LENGTH; i++) {
            for (int bit = 0; bit < Byte.SIZE; bit++) {
                bytes[i] |= bits[i * Byte.SIZE + bit] << bit;
            }
        }
        inputStream = new BitInputStream(bytes);
    }

    @Test
    public void testReadWrite() {
        BitOutputStream outputStream = new BitOutputStream();
        for (byte aByte : bytes) {
            outputStream.write(aByte & 0b11111, 5);
            outputStream.write(0b11, 2);
            outputStream.write(0b110011001100110, 15);
        }
        BitInputStream inputStream = new BitInputStream(outputStream.toByteArray());
        for (int i = 0; i < bytes.length; i++) {
            byte expected = (byte) (bytes[i] & 0b11111);
            byte actual = inputStream.readBytePartial(5);
            assertEquals("[" + i + "]" +
                    " expected: " + Integer.toBinaryString(expected) +
                    " actual: " + Integer.toBinaryString(actual), expected, actual);

            assertEquals(0b11, inputStream.readBytePartial(2));
            assertArrayEquals(new byte[]{0b1100110, 0b01100110}, inputStream.read(15));
        }
        assertEquals(0, inputStream.remainingBits());
    }

    @Test
    public void testWrite() {
        BitOutputStream outputStream = new BitOutputStream();
        for (byte bit : bits) {
            outputStream.write(bit, 1);
        }

        Assert.assertArrayEquals(bytes, outputStream.toByteArray());
    }

    @Test
    public void testRead1() {
        for (int i = 0; i < LENGTH; i++) {
//            System.out.println("[" + i + "] = " + IndexUtils.byteToString(bytes[i]));
            for (int b = 0; b < Byte.SIZE; b++) {
                byte read = inputStream.readBytePartial(1);
//                System.out.println("[" + i + "," + b + "] = " + IndexUtils.byteToString(read));
                assertEquals(i + "," + b, bits[i * 8 + b], read);
            }
        }
        thrown.expect(IndexOutOfBoundsException.class);
        thrown.expect(CoreMatchers.not(CoreMatchers.instanceOf(ArrayIndexOutOfBoundsException.class)));
        thrown.expectMessage("Bit offset request: ");
        inputStream.readBytePartial(1);
    }

    @Test
    public void testRead3() {
        for (int i = 0; i < LENGTH * Byte.SIZE - 3; i += 3) {

            byte read = inputStream.readBytePartial(3);
//            System.out.println("[" + i / 8 + "," + i % 8 + "] = " + IndexUtils.byteToString(read));
            assertEquals("[" + i / 8 + "," + i % 8 + "]", bits[i] | bits[i + 1] << 1 | bits[i + 2] << 2, read);
        }

        thrown.expect(IndexOutOfBoundsException.class);
        thrown.expect(CoreMatchers.not(CoreMatchers.instanceOf(ArrayIndexOutOfBoundsException.class)));
        thrown.expectMessage("Bit offset request: ");
        inputStream.readBytePartial(3);
    }

    @Test
    public void testReadAll() {
        Assert.assertArrayEquals(bytes, inputStream.read(LENGTH * Byte.SIZE));
    }

    @Test
    public void readMoreThanByte() {
        thrown.expect(IllegalArgumentException.class);
        inputStream.readBytePartial(10);
    }
}