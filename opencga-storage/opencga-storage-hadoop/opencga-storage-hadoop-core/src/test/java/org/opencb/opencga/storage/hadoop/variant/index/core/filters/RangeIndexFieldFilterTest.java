package org.opencb.opencga.storage.hadoop.variant.index.core.filters;

import org.junit.Test;

import static org.junit.Assert.*;

public class RangeIndexFieldFilterTest {

    @Test
    public void testNumericMethods() {
        assertTrue(RangeIndexFieldFilter.lessThan(1.0, 3.0));
        assertFalse(RangeIndexFieldFilter.lessThan(2, 2 + RangeIndexFieldFilter.DELTA - RangeIndexFieldFilter.DELTA));
        assertTrue(RangeIndexFieldFilter.equalsTo(2, 2 + RangeIndexFieldFilter.DELTA - RangeIndexFieldFilter.DELTA));
        assertNotEquals(2.0, 2 + RangeIndexFieldFilter.DELTA - RangeIndexFieldFilter.DELTA, 0);
        assertEquals(2.0, 2 + RangeIndexFieldFilter.DELTA - RangeIndexFieldFilter.DELTA, RangeIndexFieldFilter.DELTA);
    }
}