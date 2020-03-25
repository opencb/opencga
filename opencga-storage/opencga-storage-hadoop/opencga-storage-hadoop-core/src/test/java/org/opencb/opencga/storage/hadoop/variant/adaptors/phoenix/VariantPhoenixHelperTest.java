package org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix;

import org.junit.Test;

import static org.junit.Assert.*;

public class VariantPhoenixHelperTest {

    @Test
    public void testSampleColumn() {
        String column = VariantPhoenixHelper.getSampleColumn(2, 13).column();
        System.out.println(column);
        assertEquals(2, VariantPhoenixHelper.extractStudyId(column));
        assertEquals(13, VariantPhoenixHelper.extractSampleId(column));
        assertNull(VariantPhoenixHelper.extractFileIdFromSampleColumn(column, false));
    }

    @Test
    public void testSampleColumnWithFile() {
        String column = VariantPhoenixHelper.getSampleColumn(2, 13, 45).column();
        System.out.println(column);
        assertEquals(2, VariantPhoenixHelper.extractStudyId(column));
        assertEquals(13, VariantPhoenixHelper.extractSampleId(column));
        assertEquals(45, VariantPhoenixHelper.extractFileIdFromSampleColumn(column));
    }
}