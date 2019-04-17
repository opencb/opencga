package org.opencb.opencga.storage.hadoop.variant.index;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created on 17/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IndexUtilsTest {

    @Test
    public void testTestIndex() {
        assertTrue(IndexUtils.testIndex((byte) 0b00000000, (byte) 0b00000000, (byte) 0b00000000));
        assertTrue(IndexUtils.testIndex((byte) 0b00000000, (byte) 0b00000111, (byte) 0b00000000));
        assertTrue(IndexUtils.testIndex((byte) 0b00000001, (byte) 0b00000111, (byte) 0b00000001));
        assertFalse(IndexUtils.testIndex((byte) 0b00000000, (byte) 0b00000111, (byte) 0b00000001));
    }

    @Test
    public void testCountPerBit() {
        int[] expectedCounts = new int[]{2, 1, 1, 3, 2, 1, 3, 1};
        int[] counts = IndexUtils.countPerBit(new byte[]{
                (byte) 0b00000000,
                (byte) 0b00000100,
                (byte) 0b00011011,
                (byte) 0b01000001,
                (byte) 0b01111000,
                (byte) 0b11001000});
        assertArrayEquals(expectedCounts, counts);

        byte[] bytes = IndexUtils.countPerBitToBytes(counts);
        assertArrayEquals(expectedCounts, IndexUtils.countPerBitToObject(bytes));
    }
}