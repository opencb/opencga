package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SampleVariantIndexEntryTest {

    private FileIndex fileIndex;

    @Before
    public void setUp() throws Exception {
        fileIndex = SampleIndexConfiguration.defaultConfiguration().getFileIndex();
    }

    @Test
    public void testComparator() {

        List<SampleVariantIndexEntry> expected = Arrays.asList(
                newVariantIndexEntry("1:100:A:C", 0),
                newVariantIndexEntry("1:200:A:C", ((1 << 4)), true),
                newVariantIndexEntry("1:200:A:C", (2 << 4)),
                newVariantIndexEntry("1:200:A:C", (3 << 4)),
                newVariantIndexEntry("1:300:A:C", 0)
        );

        for (int i = 0; i < 10; i++) {
            ArrayList<SampleVariantIndexEntry> actual = new ArrayList<>(expected);
            Collections.shuffle(actual);

            actual.sort(SampleVariantIndexEntry::compareTo);

            Assert.assertEquals(expected, actual);
        }


    }

    protected SampleVariantIndexEntry newVariantIndexEntry(String s, int i) {
        return newVariantIndexEntry(s, i, false);
    }

    protected SampleVariantIndexEntry newVariantIndexEntry(String s, int i, boolean multiFileIndex) {
        byte[] v = new byte[4];
        Bytes.putInt(v, 0, i);
        BitBuffer fileIndex = new BitBuffer(v);
        if (multiFileIndex) {
            this.fileIndex.setMultiFile(fileIndex, 0);
        }
        return new SampleVariantIndexEntry(new Variant(s), fileIndex);
    }
}