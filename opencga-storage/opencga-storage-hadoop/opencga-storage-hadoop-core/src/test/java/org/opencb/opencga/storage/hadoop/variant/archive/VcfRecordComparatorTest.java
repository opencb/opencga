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

package org.opencb.opencga.storage.hadoop.variant.archive;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSample;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class VcfRecordComparatorTest {
    private VcfRecord sampleA = VcfRecord.newBuilder()
            .setRelativeStart(1).setRelativeEnd(2)
            .setAlternate("G").setReference("A")
            .addSamples(VcfSample.newBuilder().addSampleValues("Sample-1").build())
            .build();

    private VcfRecord sampleB = VcfRecord.newBuilder()
            .setRelativeStart(1).setRelativeEnd(3)
            .setAlternate("G").setReference("A")
            .addSamples(VcfSample.newBuilder().addSampleValues("Sample-1").build())
            .build();


    private VcfRecord sampleC = VcfRecord.newBuilder()
            .setRelativeStart(2).setRelativeEnd(3)
            .setAlternate("G").setReference("A")
            .addSamples(VcfSample.newBuilder().addSampleValues("Sample-1").build())
            .build();

    private VcfRecord sampleD = VcfRecord.newBuilder()
            .setRelativeStart(2).setRelativeEnd(3)
            .setReference("A").setAlternate("G").addSecondaryAlternates(VariantProto.AlternateCoordinate.newBuilder().setAlternate("T").build())
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
