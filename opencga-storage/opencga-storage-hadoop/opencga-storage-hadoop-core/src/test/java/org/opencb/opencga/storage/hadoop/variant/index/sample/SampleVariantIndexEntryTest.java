package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.junit.Assert;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SampleVariantIndexEntryTest {


    @Test
    public void testComparator() {

        List<SampleVariantIndexEntry> expected = Arrays.asList(
                new SampleVariantIndexEntry(new Variant("1:100:A:C"), (short) 0),
                new SampleVariantIndexEntry(new Variant("1:200:A:C"), VariantFileIndexConverter.setMultiFile((short) (1 << 4))),
                new SampleVariantIndexEntry(new Variant("1:200:A:C"), (short) (2 << 4)),
                new SampleVariantIndexEntry(new Variant("1:200:A:C"), (short) (3 << 4)),
                new SampleVariantIndexEntry(new Variant("1:300:A:C"), (short) 0)
        );

        for (int i = 0; i < 10; i++) {
            ArrayList<SampleVariantIndexEntry> actual = new ArrayList<>(expected);
            Collections.shuffle(actual);

            actual.sort(SampleVariantIndexEntry::compareTo);

            Assert.assertEquals(expected, actual);
        }


    }
}