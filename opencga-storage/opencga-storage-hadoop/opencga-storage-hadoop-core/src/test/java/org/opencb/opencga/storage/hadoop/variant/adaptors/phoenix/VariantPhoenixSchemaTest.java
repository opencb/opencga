package org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class VariantPhoenixSchemaTest {

    @Test
    public void testSampleColumn() {
        String column = VariantPhoenixSchema.getSampleColumn(2, 13).column();
        System.out.println(column);
        assertEquals(2, VariantPhoenixSchema.extractStudyId(column));
        assertEquals(13, VariantPhoenixSchema.extractSampleId(column));
        assertNull(VariantPhoenixSchema.extractFileIdFromSampleColumn(column, false));
    }

    @Test
    public void testSampleColumnWithFile() {
        String column = VariantPhoenixSchema.getSampleColumn(2, 13, 45).column();
        System.out.println(column);
        assertEquals(2, VariantPhoenixSchema.extractStudyId(column));
        assertEquals(13, VariantPhoenixSchema.extractSampleId(column));
        assertEquals(45, VariantPhoenixSchema.extractFileIdFromSampleColumn(column));
    }
}