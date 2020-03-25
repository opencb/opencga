package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opencb.biodata.models.variant.avro.VariantType.SNV;

public class SampleIndexEntryPutBuilderTest {

    @Test
    public void testContains() {
        SampleIndexEntryPutBuilder builder = new SampleIndexEntryPutBuilder(1, "1", 10);

        builder.add("0/1", new SampleVariantIndexEntry(new Variant("1:100:A:C"), (short) 20));
        builder.add("0/1", new SampleVariantIndexEntry(new Variant("1:200:A:C"), (short) 20));
        builder.add("0/1", new SampleVariantIndexEntry(new Variant("1:200:A:C"), (short) 30));
        builder.add("1/1", new SampleVariantIndexEntry(new Variant("1:300:A:C"), (short) 20));
        builder.add("1/1", new SampleVariantIndexEntry(new Variant("1:400:A:C"), (short) 20));

        assertTrue(builder.containsVariant(new SampleVariantIndexEntry(new Variant("1:100:A:C"), (short) 30)));
        assertTrue(builder.containsVariant(new SampleVariantIndexEntry(new Variant("1:200:A:C"), (short) 30)));
        assertTrue(builder.containsVariant(new SampleVariantIndexEntry(new Variant("1:300:A:C"), (short) 30)));
        assertTrue(builder.containsVariant(new SampleVariantIndexEntry(new Variant("1:400:A:C"), (short) 30)));

        assertFalse(builder.containsVariant(new SampleVariantIndexEntry(new Variant("1:50:A:C"), (short) 30)));
        assertFalse(builder.containsVariant(new SampleVariantIndexEntry(new Variant("1:500:A:C"), (short) 30)));
    }

    @Test
    public void testBuild() {
        VariantFileIndexConverter c = new VariantFileIndexConverter();

        Put build = new SampleIndexEntryPutBuilder(1, "1", 10)
                .add("0/1", new SampleVariantIndexEntry(new Variant("1:100:A:C"), c.createFileIndexValue(SNV, 0, Collections.emptyMap(), null)))
                .add("0/1", new SampleVariantIndexEntry(new Variant("1:200:A:C"), c.createFileIndexValue(SNV, 1, Collections.emptyMap(), null)))
                .add("0/1", new SampleVariantIndexEntry(new Variant("1:200:A:C"), c.createFileIndexValue(SNV, 0, Collections.emptyMap(), null)))
                .add("1/1", new SampleVariantIndexEntry(new Variant("1:300:A:C"), c.createFileIndexValue(SNV, 0, Collections.emptyMap(), null)))
                .add("1/1", new SampleVariantIndexEntry(new Variant("1:400:A:C"), c.createFileIndexValue(SNV, 0, Collections.emptyMap(), null)))
                .build();


    }



}