package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class VarIntDataFieldTest {

    private VarIntDataField field;

    @Before
    public void setUp() {
        field = new VarIntDataField(null);
    }

    @Test
    public void shouldHandlePositiveValues() {
        testVarint(5, 1);
    }

    @Test
    public void shouldHandlePositive2bValues() {
        testVarint(0x0600, 2);
    }

    @Test
    public void shouldHandlePositive3bValues() {
        testVarint(0x060000, 3);
    }

    @Test
    public void shouldHandlePositive4bValues() {
        testVarint(0x06000000, 4);
    }


    @Test
    public void shouldHandleAllValues() {
        int prevLength = 0;
        for (int expected = -2000000; expected < 4000000; expected++) {
            int thisLength = field.getByteLength(expected);
            if (expected % 1000000 == 0 || prevLength != thisLength) {
                System.out.println(expected + " _ " + thisLength);
            }
            testVarint(expected, thisLength);
            prevLength = thisLength;
        }
    }

    @Test
    public void shouldHandleNegativeValues() {
        testVarint(-5, 5);
    }

    @Test
    public void shouldHandleZero() {
        testVarint(0, 1);
    }

    @Test
    public void shouldHandleMaximumInteger() {
        testVarint(Integer.MAX_VALUE, 5);
    }

    @Test
    public void shouldHandleMinimumInteger() {
        testVarint(Integer.MIN_VALUE, 5);
    }

    private void testVarint(int expected, int expectedLenght) {
        int actualLength = field.getByteLength(expected);
        Assert.assertEquals(expectedLenght, actualLength);
        ByteBuffer buffer = ByteBuffer.allocate(actualLength);
        field.write(expected, buffer);
        ByteBuffer buffer2 = field.encode(expected);

        buffer.rewind();
        Assert.assertEquals(buffer, buffer2);

        int actualOutput = field.readAndDecode(buffer);
        Assert.assertEquals(expected, actualOutput);
        Assert.assertEquals(0, buffer.remaining());

        buffer.rewind();
        actualOutput = field.decode(field.read(buffer));
        Assert.assertEquals(expected, actualOutput);
        Assert.assertEquals(0, buffer.remaining());
    }


}