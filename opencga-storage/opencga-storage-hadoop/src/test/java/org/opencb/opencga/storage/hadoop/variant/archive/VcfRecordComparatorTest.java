package org.opencb.opencga.storage.hadoop.variant.archive;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSample;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class VcfRecordComparatorTest {
    private VcfRecord sampleA = VcfRecord.newBuilder()
            .setRelativeStart(1).setRelativeEnd(2)
            .addAlternate("G").setReference("A")
            .addSamples(VcfSample.newBuilder().addSampleValues("Sample-1").build())
            .build();

    private VcfRecord sampleB = VcfRecord.newBuilder()
            .setRelativeStart(1).setRelativeEnd(3)
            .addAlternate("G").setReference("A")
            .addSamples(VcfSample.newBuilder().addSampleValues("Sample-1").build())
            .build();


    private VcfRecord sampleC = VcfRecord.newBuilder()
            .setRelativeStart(2).setRelativeEnd(3)
            .addAlternate("G").setReference("A")
            .addSamples(VcfSample.newBuilder().addSampleValues("Sample-1").build())
            .build();

    private VcfRecord sampleD = VcfRecord.newBuilder()
            .setRelativeStart(2).setRelativeEnd(3)
            .setReference("A").addAlternate("G").addAlternate("T")
            .addSamples(VcfSample.newBuilder().addSampleValues("Sample-1").build())
            .build();

    List<VcfRecord> lst;

    @Before
    public void setUp() throws Exception {
        lst = Arrays.asList(
                sampleB,
                sampleC,
                sampleA,
                sampleD
        );
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCompareEqual() {
        assertEquals(0, new VcfRecordComparator().compare(sampleA, sampleA));
    }

    @Test
    public void testCompareNotEqual() {
        assertNotEquals(0, new VcfRecordComparator().compare(sampleA, sampleB));
    }

    @Test
    public void testCompare() {
        Collections.sort(lst, new VcfRecordComparator());
        assertTrue(sampleA.equals(lst.get(0)));
    }

}
