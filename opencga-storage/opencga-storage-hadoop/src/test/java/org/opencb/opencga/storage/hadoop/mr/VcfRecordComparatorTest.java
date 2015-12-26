/**
 *
 */
package org.opencb.opencga.storage.hadoop.mr;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;
import org.opencb.opencga.storage.hadoop.variant.archive.VcfRecordComparator;

import static org.junit.Assert.assertEquals;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VcfRecordComparatorTest {


    private VcfRecord a1;
    private VcfRecord a2;
    private VcfRecord b1;
    private VcfRecord b2;
    private VcfRecordComparator vcfRecordComparator;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        a1 = VcfRecord.newBuilder().setRelativeStart(1).setRelativeEnd(2).addAlternate("a").build();
        a2 = VcfRecord.newBuilder().setRelativeStart(2).setRelativeEnd(2).addAlternate("a").build();
        b1 = VcfRecord.newBuilder().setRelativeStart(1).setRelativeEnd(3).addAlternate("b").build();
        b2 = VcfRecord.newBuilder().setRelativeStart(1).setRelativeEnd(2).addAlternate("b").build();
        vcfRecordComparator = new VcfRecordComparator();
    }

    /**
     * Test method for {@link org.opencb.opencga.storage.hadoop.variant.archive.VcfRecordComparator#compare(org.opencb.biodata.models
     * .variant.protobuf.VcfSliceProtos.VcfRecord, org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord)}.
     */
    @Test
    public void testStart() {
        int comp = vcfRecordComparator.compare(a1, a2);
        assertEquals(-1, comp);
    }

    @Test
    public void testEqual() {
        int comp = vcfRecordComparator.compare(a1, a1);
        assertEquals(0, comp);
    }

    @Test
    public void testLarger() {
        int comp = vcfRecordComparator.compare(a2, a1);
        assertEquals(1, comp);
    }

    @Test
    public void testEnd() {
        int comp = vcfRecordComparator.compare(a1, b1);
        assertEquals(-1, comp);
    }


    @Test
    public void testAlt() {
        int comp = vcfRecordComparator.compare(a1, b2);
        assertEquals(-1, comp);
    }


}
