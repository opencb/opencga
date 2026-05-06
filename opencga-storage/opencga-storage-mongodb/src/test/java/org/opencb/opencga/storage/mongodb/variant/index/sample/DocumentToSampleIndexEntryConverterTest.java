package org.opencb.opencga.storage.mongodb.variant.index.sample;

import org.bson.Document;
import org.junit.Test;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;

import java.util.Collections;

import static org.junit.Assert.*;

public class DocumentToSampleIndexEntryConverterTest {

    private final DocumentToSampleIndexEntryConverter converter = new DocumentToSampleIndexEntryConverter();

    @Test
    public void testRoundTrip() {
        SampleIndexEntry entry = new SampleIndexEntry(5, "1", 1000);
        entry.setDiscrepancies(2);
        entry.setMendelianVariants("abc".getBytes(), 0, 3);
        SampleIndexEntry.SampleIndexGtEntry gtEntry = entry.getGtEntry("0/1");
        gtEntry.setCount(7);
        gtEntry.setVariants("var".getBytes(), 0, 3);
        gtEntry.setAnnotationCounts(new int[]{1, 2});

        Document doc = converter.convertToStorageType(entry);
        assertEquals(DocumentToSampleIndexEntryConverter.buildDocumentId(entry), doc.getString("_id"));
        assertEquals(7, doc.get("gt_0v1_count"));
        assertTrue(doc.containsKey("gt_0v1_variants"));

        SampleIndexEntry decoded = converter.convertToDataModelType(doc);
        assertEquals(entry.getSampleId(), decoded.getSampleId());
        assertEquals(2, decoded.getDiscrepancies());
        assertArrayEquals(entry.getMendelianVariantsValue(), decoded.getMendelianVariantsValue());
        assertTrue(decoded.getGts().containsKey("0/1"));
        SampleIndexEntry.SampleIndexGtEntry decodedGt = decoded.getGts().get("0/1");
        assertEquals(7, decodedGt.getCount());
        assertArrayEquals(gtEntry.getVariants(), decodedGt.getVariants());
        assertArrayEquals(gtEntry.getAnnotationCounts(), decodedGt.getAnnotationCounts());
    }

    @Test
    public void testPartialUpdateContainsOnlyProvidedFields() {
        SampleIndexEntry entry = new SampleIndexEntry(10, "X", 2000);
        entry.setDiscrepancies(0);
        entry.setGts(Collections.emptyMap());
        Document doc = converter.convertToStorageType(entry);
        assertFalse(doc.isEmpty());
        assertEquals(DocumentToSampleIndexEntryConverter.buildDocumentId(entry), doc.getString("_id"));
        assertEquals(0, doc.get("discrepancies"));
        assertFalse(doc.containsKey("gt_"));
    }
}
