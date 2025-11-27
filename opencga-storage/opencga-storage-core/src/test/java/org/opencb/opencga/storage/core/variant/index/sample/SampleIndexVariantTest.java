package org.opencb.opencga.storage.core.variant.index.sample;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.thirdparty.hbase.util.Bytes;
import org.opencb.opencga.storage.core.variant.index.sample.schema.FileIndexSchema;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Category(ShortTests.class)
public class SampleIndexVariantTest {

    private FileIndexSchema fileIndex;

    @Before
    public void setUp() throws Exception {
        fileIndex = SampleIndexSchema.defaultSampleIndexSchema().getFileIndex();
    }

    @Test
    public void testComparator() {

        List<SampleIndexVariant> expected = Arrays.asList(
                newVariantIndexEntry("1:100:A:C", 0),
                newVariantIndexEntry("1:200:A:C", ((1 << 4)), true),
                newVariantIndexEntry("1:200:A:C", (2 << 4)),
                newVariantIndexEntry("1:200:A:C", (3 << 4)),
                newVariantIndexEntry("1:300:A:C", 0)
        );

        for (int i = 0; i < 10; i++) {
            ArrayList<SampleIndexVariant> actual = new ArrayList<>(expected);
            Collections.shuffle(actual);

            actual.sort(new SampleIndexVariant.SampleIndexVariantComparator(SampleIndexSchema.defaultSampleIndexSchema()));

            Assert.assertEquals(expected, actual);
        }


    }

    protected SampleIndexVariant newVariantIndexEntry(String s, int i) {
        return newVariantIndexEntry(s, i, false);
    }

    protected SampleIndexVariant newVariantIndexEntry(String s, int i, boolean multiFileIndex) {
        byte[] v = new byte[4];
        Bytes.putInt(v, 0, i);
        BitBuffer fileIndex = new BitBuffer(v);
        if (multiFileIndex) {
            this.fileIndex.setMultiFile(fileIndex, 0);
        }
        return new SampleIndexVariant(new Variant(s), fileIndex, null);
    }
}