package org.opencb.opencga.storage.hadoop.variant.mr;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.io.*;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class VariantLocusKeyTest {

    @Test
    public void shouldReturnTrueForEqualVariantLocusKeys() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1000);
        VariantLocusKey key2 = new VariantLocusKey("1", 1000);
        assertTrue(key1.equals(key2));
    }

    @Test
    public void shouldReturnFalseForDifferentVariantLocusKeys() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1000);
        VariantLocusKey key2 = new VariantLocusKey("2", 1000);
        assertFalse(key1.equals(key2));
    }

    @Test
    public void shouldReturnFalseForNullVariantLocusKey() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1000);
        assertFalse(key1.equals(null));
    }

    @Test
    public void shouldReturnFalseForDifferentObjectType() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1000);
        String otherObject = "someString";
        assertFalse(key1.equals(otherObject));
    }

    @Test
    public void shouldReturnConsistentHashCodeForEqualVariantLocusKeys() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1000);
        VariantLocusKey key2 = new VariantLocusKey("1", 1000);
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    public void shouldReturnDifferentHashCodeForDifferentVariantLocusKeys() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1000);
        VariantLocusKey key2 = new VariantLocusKey("2", 1000);
        assertNotEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    public void shouldReturnZeroForEqualVariantLocusKeys() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1000, "A");
        VariantLocusKey key2 = new VariantLocusKey("1", 1000, "A");
        assertEquals(0, key1.compareTo(key2));
    }

    @Test
    public void shouldReturnNegativeForSmallerChromosome() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1000, "A");
        VariantLocusKey key2 = new VariantLocusKey("2", 1000, "A");
        assertTrue(key1.compareTo(key2) < 0);
    }

    @Test
    public void shouldReturnPositiveForLargerChromosome() {
        VariantLocusKey key1 = new VariantLocusKey("2", 1000, "A");
        VariantLocusKey key2 = new VariantLocusKey("1", 1000, "A");
        assertTrue(key1.compareTo(key2) > 0);
    }

    @Test
    public void shouldReturnNegativeForSmallerPosition() {
        VariantLocusKey key1 = new VariantLocusKey("1", 999, "A");
        VariantLocusKey key2 = new VariantLocusKey("1", 1000, "A");
        assertTrue(key1.compareTo(key2) < 0);
    }

    @Test
    public void shouldReturnPositiveForLargerPosition() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1001, "A");
        VariantLocusKey key2 = new VariantLocusKey("1", 1000, "A");
        assertTrue(key1.compareTo(key2) > 0);
    }

    @Test
    public void shouldReturnNegativeForSmallerOther() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1000, "A");
        VariantLocusKey key2 = new VariantLocusKey("1", 1000, "B");
        assertTrue(key1.compareTo(key2) < 0);
    }

    @Test
    public void shouldReturnPositiveForLargerOther() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1000, "B");
        VariantLocusKey key2 = new VariantLocusKey("1", 1000, "A");
        assertTrue(key1.compareTo(key2) > 0);
    }

    @Test
    public void shouldReturnZeroWhenBothOtherAreNull() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1000, null);
        VariantLocusKey key2 = new VariantLocusKey("1", 1000, null);
        assertEquals(0, key1.compareTo(key2));
    }

    @Test
    public void shouldReturnNegativeWhenOtherIsNull() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1000, null);
        VariantLocusKey key2 = new VariantLocusKey("1", 1000, "A");
        assertTrue(key1.compareTo(key2) < 0);
    }

    @Test
    public void shouldReturnPositiveWhenOtherIsNotNull() {
        VariantLocusKey key1 = new VariantLocusKey("1", 1000, "A");
        VariantLocusKey key2 = new VariantLocusKey("1", 1000, null);
        assertTrue(key1.compareTo(key2) > 0);
    }

    @Test
    public void shouldCompareChromosomesCorrectly() {
        List<VariantLocusKey> keys = Arrays.asList(
                new VariantLocusKey("1", 1000, "A"),
                new VariantLocusKey("1_random", 1000, "A"),
                new VariantLocusKey("2", 1000, "A"),
                new VariantLocusKey("9", 1000, "A"),
                new VariantLocusKey("10", 1000, "A"),
                new VariantLocusKey("10_random", 1000, "A"),
                new VariantLocusKey("19", 1000, "A"),
                new VariantLocusKey("20", 1000, "A"),
                new VariantLocusKey("22", 1000, "A"),
                new VariantLocusKey("X", 1000, "A"),
                new VariantLocusKey("Y", 1000, "A")
        );

        VariantLocusKey prevKey = null;
        for (VariantLocusKey key : keys) {
            if (prevKey == null) {
                prevKey = key;
            } else {
                assertTrue(prevKey + " < " + key, prevKey.compareTo(key) < 0);
                prevKey = key;
            }
        }
    }

    @Test
    public void testWriteAndRead() throws IOException {
        testWriteAndRead(new VariantLocusKey("1_random", 1000, "A"));
        testWriteAndRead(new VariantLocusKey("1", 3541316, "O:31231"));
        testWriteAndRead(new VariantLocusKey("0", 3541316, "O:31231"));
        testWriteAndRead(new VariantLocusKey("", 3541316, ""));
        testWriteAndRead(new VariantLocusKey("", -2, ""));
    }

    private static void testWriteAndRead(VariantLocusKey originalKey) throws IOException {
        // Write the object to a byte array output stream
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        originalKey.write(dataOutputStream);

        // Read the object from a byte array input stream
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
        VariantLocusKey readKey = new VariantLocusKey();
        readKey.readFields(dataInputStream);

        // Assert that the read object is equal to the original object
        assertEquals(originalKey, readKey);
    }
}