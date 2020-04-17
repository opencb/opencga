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

package org.opencb.opencga.storage.hadoop.variant.converters.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created on 11/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsToHBaseConverterTest {

    private VariantStatsToHBaseConverter toHbase;
    private HBaseToVariantStatsConverter toJava;
    private GenomeHelper genomeHelper;

    @Before
    public void setUp() {

        StudyMetadata studyMetadata = new StudyMetadata().setId(1).setName("s1");
        Map<String, Integer> cohortIds = new HashMap<>();
        cohortIds.put("c1", 1);
        cohortIds.put("c2", 2);

        genomeHelper = new GenomeHelper(new Configuration());
        toHbase = new VariantStatsToHBaseConverter(studyMetadata, cohortIds);
        toJava = new HBaseToVariantStatsConverter();
    }

    @Test
    public void test() throws IOException {
        VariantStatsWrapper statsWrapper = new VariantStatsWrapper();
        statsWrapper.setChromosome("1");
        statsWrapper.setStart(100);
        statsWrapper.setReference("A");
        statsWrapper.setAlternate("C");
        VariantStats expected = new VariantStats("c1");
        expected.setAltAlleleCount(1);
        expected.setAltAlleleFreq(0.1F);
        expected.setRefAlleleCount(2);
        expected.setRefAlleleFreq(0.2F);
        expected.setMissingAlleleCount(3);
        expected.setMissingGenotypeCount(6);
        expected.setMaf(0.4F);
        expected.setMafAllele("A");
        expected.setMgf(0.5F);
        expected.setMgfGenotype("0/1");
        expected.addGenotype(new Genotype("0/0"), 10, false);
        expected.addGenotype(new Genotype("0/1"), 20, false);
        expected.addGenotype(new Genotype("1/1"), 30, false);
        statsWrapper.setCohortStats(Collections.singletonList(expected));
        Put put = toHbase.convert(statsWrapper);

        List<Cell> cells = put.getFamilyCellMap().get(GenomeHelper.COLUMN_FAMILY_BYTES);

        VariantStats convert = null;
        for (Cell cell : cells) {
            byte[] value = CellUtil.cloneValue(cell);
            String col = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (col.endsWith(VariantPhoenixHelper.COHORT_STATS_PROTOBUF_SUFFIX)) {
                convert = toJava.convert(value);
            }
        }

        assertNotNull(convert);
        assertEquals("", convert.getCohortId());
        convert.setCohortId("c1");
        assertEquals(expected, convert);

    }


}