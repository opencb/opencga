package org.opencb.opencga.storage.core.variant.index.sample.file;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.thirdparty.hbase.util.Bytes;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opencb.biodata.models.variant.avro.VariantType.INDEL;
import static org.opencb.biodata.models.variant.avro.VariantType.SNV;

@Category(ShortTests.class)
public class SampleIndexEntryBuilderTest {

    @Test
    public void testContains() {
        SampleIndexEntryBuilder builder = new SampleIndexEntryBuilder(1, "1", 10, SampleIndexSchema.defaultSampleIndexSchema(), false, true);

        builder.add("0/1", newVariantIndexEntry("1:100:A:C", (short) 20));
        builder.add("0/1", newVariantIndexEntry("1:200:A:C", (short) 20));
        builder.add("0/1", newVariantIndexEntry("1:200:A:C", (short) 30));
        builder.add("1/1", newVariantIndexEntry("1:300:A:C", (short) 20));
        builder.add("1/1", newVariantIndexEntry("1:400:A:C", (short) 20));

        assertTrue(builder.containsVariant(newVariantIndexEntry("1:100:A:C", (short) 30)));
        assertTrue(builder.containsVariant(newVariantIndexEntry("1:200:A:C", (short) 30)));
        assertTrue(builder.containsVariant(newVariantIndexEntry("1:300:A:C", (short) 30)));
        assertTrue(builder.containsVariant(newVariantIndexEntry("1:400:A:C", (short) 30)));

        assertFalse(builder.containsVariant(newVariantIndexEntry("1:50:A:C", (short) 30)));
        assertFalse(builder.containsVariant(newVariantIndexEntry("1:500:A:C", (short) 30)));
    }

    protected SampleIndexVariant newVariantIndexEntry(String s, short i) {
        byte[] v = new byte[2];
        Bytes.putShort(v, 0, i);
        return new SampleIndexVariant(new Variant(s), new BitBuffer(v), null);
    }

    @Test
    public void testBuild() {
        SampleIndexSchema schema = SampleIndexSchema.defaultSampleIndexSchema();
        SampleIndexVariantConverter c = new SampleIndexVariantConverter(schema);

        SampleIndexEntryBuilder builder = new SampleIndexEntryBuilder(1, "1", 10, schema, false, true);
        assertTrue(builder.add("0/1", new SampleIndexVariant(new Variant("1:100:A:C"), c.createFileIndexValue(SNV, 0, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList()), null)));
        assertTrue(builder.add("0/1", new SampleIndexVariant(new Variant("1:200:A:C"), c.createFileIndexValue(SNV, 1, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList()), null)));
        assertTrue(builder.add("0/1", new SampleIndexVariant(new Variant("1:200:A:C"), c.createFileIndexValue(SNV, 0, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList()), null)));
        assertTrue(builder.add("1/1", new SampleIndexVariant(new Variant("1:300:A:C"), c.createFileIndexValue(SNV, 0, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList()), null)));
        assertTrue(builder.add("1/1", new SampleIndexVariant(new Variant("1:400:A:C"), c.createFileIndexValue(SNV, 0, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList()), null)));

        assertFalse(builder.add("0/1", new SampleIndexVariant(new Variant("1:200:A:C"), c.createFileIndexValue(SNV, 0, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList()), null)));
        assertTrue(builder.add("0/1", new SampleIndexVariant(new Variant("1:200:A:C"), c.createFileIndexValue(INDEL, 0, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList()), null)));
        assertFalse(builder.add("0/1", new SampleIndexVariant(new Variant("1:200:A:C"), c.createFileIndexValue(INDEL, 0, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList()), null)));


        SampleIndexEntry entry = builder.buildEntry();
    }

}