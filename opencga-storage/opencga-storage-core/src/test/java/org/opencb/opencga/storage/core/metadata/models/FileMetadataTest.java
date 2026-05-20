package org.opencb.opencga.storage.core.metadata.models;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import static org.junit.Assert.assertEquals;

@Category(ShortTests.class)
public class FileMetadataTest {

    @Test
    public void testAnnotationSetIdRoundTrip() {
        FileMetadata fm = new FileMetadata(1, 100, "f1.vcf");

        // Unset -> 0 (treated as fresh for backcompat in discovery).
        assertEquals(0, fm.getAnnotationSetId());

        fm.setAnnotationSetId(3);
        assertEquals(3, fm.getAnnotationSetId());

        fm.setAnnotationSetId(7);
        assertEquals(7, fm.getAnnotationSetId());
    }
}
