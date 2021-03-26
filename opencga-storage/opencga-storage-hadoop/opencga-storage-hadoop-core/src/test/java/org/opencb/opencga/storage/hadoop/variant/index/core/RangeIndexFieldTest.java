package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.junit.Test;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;

import static org.junit.Assert.*;
import static org.opencb.opencga.core.config.storage.IndexFieldConfiguration.Source.SAMPLE;

public class RangeIndexFieldTest {

    @Test
    public void testLength() {
        boolean nullable = false;
        assertEquals(1, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{1}).setNullable(nullable), 0).getBitLength());
        assertEquals(2, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{1, 2}).setNullable(nullable), 0).getBitLength());
        assertEquals(2, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{2, 4, 5}).setNullable(nullable), 0).getBitLength());
        assertEquals(3, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{2, 4, 5, 6}).setNullable(nullable), 0).getBitLength());
        assertEquals(3, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{1, 2, 3, 4, 5, 6, 7}).setNullable(nullable), 0).getBitLength());
        assertEquals(4, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{1, 2, 3, 4, 5, 6, 7, 8}).setNullable(nullable), 0).getBitLength());

        nullable = true;
        assertEquals(2, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{1}).setNullable(nullable), 0).getBitLength());
        assertEquals(2, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{2, 4}).setNullable(nullable), 0).getBitLength());
        assertEquals(3, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{2, 4, 5}).setNullable(nullable), 0).getBitLength());
        assertEquals(3, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{1, 2, 2, 3}).setNullable(nullable), 0).getBitLength());
        assertEquals(3, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{1, 2, 2, 3, 4}).setNullable(nullable), 0).getBitLength());
        assertEquals(3, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{1, 2, 2, 3, 4, 5}).setNullable(nullable), 0).getBitLength());
        assertEquals(4, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{1, 2, 2, 3, 4, 5, 6}).setNullable(nullable), 0).getBitLength());
    }

    @Test
    public void testNumericMethods() {
        assertTrue(RangeIndexField.lessThan(1.0, 3.0));
        assertFalse(RangeIndexField.lessThan(2, 2 + RangeIndexField.DELTA - RangeIndexField.DELTA));
        assertTrue(RangeIndexField.equalsTo(2, 2 + RangeIndexField.DELTA - RangeIndexField.DELTA));
        assertNotEquals(2.0, 2 + RangeIndexField.DELTA - RangeIndexField.DELTA, 0);
        assertEquals(2.0, 2 + RangeIndexField.DELTA - RangeIndexField.DELTA, RangeIndexField.DELTA);
    }
}