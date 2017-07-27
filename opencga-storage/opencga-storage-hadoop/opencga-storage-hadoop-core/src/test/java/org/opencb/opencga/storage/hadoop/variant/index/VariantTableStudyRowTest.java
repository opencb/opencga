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

package org.opencb.opencga.storage.hadoop.variant.index;

import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.SampleList;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.*;

/**
 * Created on 07/03/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantTableStudyRowTest {

    @Test
    public void testCreateFromVariant() throws Exception {

        Variant variant = new Variant("1:1000:A:T");
        Integer studyId = 1;

        StudyEntry studyEntry = new StudyEntry(studyId.toString(), Collections.emptyList(), Arrays.asList("GT", VariantMerger.VCF_FILTER));
        studyEntry.addSampleData("s1", Arrays.asList("0/1", "PASS"));
        studyEntry.addSampleData("s2", Arrays.asList("0/0", ""));
        studyEntry.addSampleData("s3", Arrays.asList("1/1", "-"));
        studyEntry.addSampleData("s4", Arrays.asList("0/0", "High"));
        studyEntry.addSampleData("s5", Arrays.asList("0/0", "Low"));
        studyEntry.addSampleData("s6", Arrays.asList(".", "."));
        variant.addStudyEntry(studyEntry);

        VariantTableStudyRow row = new VariantTableStudyRow(variant, studyId, studyEntry.getSamplesPosition());

        assertEquals("1:1000:A:T", row.toString());

        // Check GT
        assertEquals(3, row.getHomRefCount().intValue());
        assertEquals(5, row.getCallCount().intValue());
        assertEquals(Collections.singleton(0), row.getSampleIds(VariantTableStudyRow.HET_REF));
        assertEquals(Collections.singleton(2), row.getSampleIds(VariantTableStudyRow.HOM_VAR));
        assertEquals(Collections.singleton(5), row.getSampleIds(VariantTableStudyRow.NOCALL));

        // Check FILTER
        assertEquals(1, row.getPassCount().intValue());
        assertEquals(3, row.getComplexFilter().getFilterNonPass().size());
        int otherFilters = row.getComplexFilter().getFilterNonPass().values()
                .stream()
                .map(SampleList::getSampleIdsCount)
                .reduce((i1, i2) -> i1 + i2).orElse(0);
        assertEquals(5, otherFilters);
        assertEquals(6, otherFilters + row.getPassCount());

        System.out.println("row = " + row.toSummaryString());
        System.out.println("row = " + row);

    }
}