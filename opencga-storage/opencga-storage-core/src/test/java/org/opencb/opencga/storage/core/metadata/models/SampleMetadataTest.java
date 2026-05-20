package org.opencb.opencga.storage.core.metadata.models;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import static org.junit.Assert.assertEquals;

@Category(ShortTests.class)
public class SampleMetadataTest {

    @Test
    public void testAnnotationSetIdRoundTrip() {
        SampleMetadata sm = new SampleMetadata(1, 100, "S1");

        // Unset -> 0 (means "unknown / assume current" for backcompat).
        assertEquals(0, sm.getSampleIndexAnnotationSetId(2));

        sm.setSampleIndexAnnotationSetId(7, 2);
        assertEquals(7, sm.getSampleIndexAnnotationSetId(2));

        // Independent per SSI version.
        assertEquals(0, sm.getSampleIndexAnnotationSetId(3));
        sm.setSampleIndexAnnotationSetId(8, 3);
        assertEquals(7, sm.getSampleIndexAnnotationSetId(2));
        assertEquals(8, sm.getSampleIndexAnnotationSetId(3));

        // Overwrite same version.
        sm.setSampleIndexAnnotationSetId(9, 2);
        assertEquals(9, sm.getSampleIndexAnnotationSetId(2));
    }

    @Test
    public void testGeneralAnnotationSetIdRoundTrip() {
        SampleMetadata sm = new SampleMetadata(1, 100, "S1");

        // Unset -> 0 (treated as fresh for backcompat in discovery).
        assertEquals(0, sm.getAnnotationSetId());

        sm.setAnnotationSetId(5);
        assertEquals(5, sm.getAnnotationSetId());

        // Independent of per-SSI-version stamps.
        sm.setSampleIndexAnnotationSetId(7, 2);
        assertEquals(5, sm.getAnnotationSetId());
        assertEquals(7, sm.getSampleIndexAnnotationSetId(2));
    }
}
