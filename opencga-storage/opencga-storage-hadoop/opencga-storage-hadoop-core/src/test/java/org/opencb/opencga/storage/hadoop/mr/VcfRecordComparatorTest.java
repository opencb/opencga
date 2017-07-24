/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        a1 = VcfRecord.newBuilder().setRelativeStart(1).setRelativeEnd(2).setAlternate("a").build();
        a2 = VcfRecord.newBuilder().setRelativeStart(2).setRelativeEnd(2).setAlternate("a").build();
        b1 = VcfRecord.newBuilder().setRelativeStart(1).setRelativeEnd(3).setAlternate("b").build();
        b2 = VcfRecord.newBuilder().setRelativeStart(1).setRelativeEnd(2).setAlternate("b").build();
        vcfRecordComparator = new VcfRecordComparator();
    }

    /**
     * Test method for {@link VcfRecordComparator#compare(VcfRecord, VcfRecord)}.
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
