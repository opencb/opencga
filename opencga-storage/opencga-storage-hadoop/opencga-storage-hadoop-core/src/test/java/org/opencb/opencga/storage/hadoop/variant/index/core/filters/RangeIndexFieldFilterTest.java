package org.opencb.opencga.storage.hadoop.variant.index.core.filters;

import org.junit.Test;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.storage.core.variant.query.OpValue;
import org.opencb.opencga.storage.hadoop.variant.index.core.RangeIndexField;

import static org.junit.Assert.*;
import static org.opencb.opencga.core.config.storage.IndexFieldConfiguration.Source.SAMPLE;

public class RangeIndexFieldFilterTest {

    @Test
    public void testExactLtNonNullable() {
        testExactLt(false);
    }

    @Test
    public void testExactLtNullable() {
        testExactLt(true);
    }

    @Test
    public void testExactGtNonNullable() {
        testExactGt(false);
    }

    @Test
    public void testExactGtNullable() {
        testExactGt(true);
    }

    public void testExactLt(boolean nullable) {
        RangeIndexField field = new RangeIndexField(
                new IndexFieldConfiguration(SAMPLE, "K", new double[]{1, 2, 2, 3}).setNullable(nullable), 0);


        assertFalse(field.buildFilter(new OpValue<>("==", 1d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("==", 1.999d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>("==", 2d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("==", 2.001d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("==", 3d)).isExactFilter());


        assertFalse(field.buildFilter(new OpValue<>(">=", 0d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>(">=", 1d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">=", 1.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>(">=", 2d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">=", 2.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>(">=", 3d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">=", 3.5d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">=", 8d)).isExactFilter());


        assertFalse(field.buildFilter(new OpValue<>("<", 4d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<", 3.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>("<", 3d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<", 2.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>("<", 2d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<", 1.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>("<", 1d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<", 0d)).isExactFilter());

        assertFalse(field.buildFilter(new OpValue<>(">", 0d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">", 1d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">", 1.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>(">", 2d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">", 2.5d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">", 3d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">", 3.5d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">", 8d)).isExactFilter());


        assertFalse(field.buildFilter(new OpValue<>("<=", 4d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<=", 3.5d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<=", 3d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<=", 2.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>("<=", 2d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<=", 1.5d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<=", 1d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<=", 0d)).isExactFilter());
    }

    public void testExactGt(boolean nullable) {
        RangeIndexField field = new RangeIndexField(
                new IndexFieldConfiguration(SAMPLE, "K", new double[]{1, 2, 2, 3},
                        IndexFieldConfiguration.Type.RANGE_GT).setNullable(nullable), 0);

        assertFalse(field.buildFilter(new OpValue<>("==", 1d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("==", 1.999d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>("==", 2d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("==", 2.001d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("==", 3d)).isExactFilter());

        assertFalse(field.buildFilter(new OpValue<>(">", 0d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>(">", 1d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">", 1.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>(">", 2d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">", 2.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>(">", 3d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">", 3.5d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">", 8d)).isExactFilter());


        assertFalse(field.buildFilter(new OpValue<>("<=", 4d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<=", 3.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>("<=", 3d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<=", 2.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>("<=", 2d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<=", 1.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>("<=", 1d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<=", 0d)).isExactFilter());

        assertFalse(field.buildFilter(new OpValue<>(">=", 0d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">=", 1d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">=", 1.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>(">=", 2d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">=", 2.5d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">=", 3d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">=", 3.5d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>(">=", 8d)).isExactFilter());

        assertFalse(field.buildFilter(new OpValue<>("<", 4d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<", 3.5d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<", 3d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<", 2.5d)).isExactFilter());
        assertTrue(field.buildFilter(new OpValue<>("<", 2d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<", 1.5d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<", 1d)).isExactFilter());
        assertFalse(field.buildFilter(new OpValue<>("<", 0d)).isExactFilter());
    }

    @Test
    public void testLength() {
        assertEquals(1, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{1}).setNullable(false), 0).getBitLength());
        assertEquals(2, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{1}).setNullable(true), 0).getBitLength());
        assertEquals(2, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{2, 4}).setNullable(true), 0).getBitLength());
        assertEquals(2, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{2, 4, 5}).setNullable(false), 0).getBitLength());
        assertEquals(3, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{2, 4, 5}).setNullable(true), 0).getBitLength());
        assertEquals(3, new RangeIndexField(new IndexFieldConfiguration(SAMPLE, "K", new double[]{1, 2, 2, 3}).setNullable(true), 0).getBitLength());
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