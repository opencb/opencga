package org.opencb.opencga.storage.hadoop.variant.converters;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created on 20/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AbstractPhoenixConverterTest {

    @Test
    public void testEndsWith() {
        byte[] bytes = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        assertTrue(AbstractPhoenixConverter.endsWith(bytes, new byte[]{8, 9}));
        assertTrue(AbstractPhoenixConverter.endsWith(bytes, new byte[]{7, 8, 9}));
        assertFalse(AbstractPhoenixConverter.endsWith(bytes, new byte[]{7, 8, 9, 10}));
        assertFalse(AbstractPhoenixConverter.endsWith(bytes, new byte[]{0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

        assertTrue(AbstractPhoenixConverter.endsWith(bytes, 0, 5, new byte[]{3, 4}));
        assertTrue(AbstractPhoenixConverter.endsWith(bytes, 0, 5, new byte[]{2, 3, 4}));
        assertFalse(AbstractPhoenixConverter.endsWith(bytes, 0, 5, new byte[]{3, 4, 5}));

        assertTrue(AbstractPhoenixConverter.endsWith(bytes, 4, 3, new byte[]{5, 6}));
        assertTrue(AbstractPhoenixConverter.endsWith(bytes, 4, 3, new byte[]{4, 5, 6}));
        assertFalse(AbstractPhoenixConverter.endsWith(bytes, 4, 3, new byte[]{1, 1}));
        assertFalse(AbstractPhoenixConverter.endsWith(bytes, 4, 3, new byte[]{1, 1, 1, 1, 1}));
    }
}