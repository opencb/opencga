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
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

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

        StudyConfiguration studyConfiguration = new StudyConfiguration(1, "s1");
        studyConfiguration.getCohortIds().put("c1", 1);
        studyConfiguration.getCohortIds().put("c2", 2);

        genomeHelper = new GenomeHelper(new Configuration());
        toHbase = new VariantStatsToHBaseConverter(genomeHelper, studyConfiguration);
        toJava = new HBaseToVariantStatsConverter(genomeHelper);

    }

    @Test
    public void test() throws IOException {
        VariantStatsWrapper statsWrapper = new VariantStatsWrapper();
        statsWrapper.setChromosome("1");
        statsWrapper.setPosition(100);
        HashMap<String, VariantStats> map = new HashMap<>();
        VariantStats expected = new VariantStats("A", "C", VariantType.SNV);
        expected.setAltAlleleCount(1);
        expected.setAltAlleleFreq(0.1F);
        expected.setRefAlleleCount(2);
        expected.setRefAlleleFreq(0.2F);
        expected.setMissingAlleles(3);
        expected.setMissingGenotypes(6);
        expected.setMaf(0.4F);
        expected.setMafAllele("A");
        expected.setMgf(0.5F);
        expected.setMgfGenotype("0/1");
        expected.addGenotype(new Genotype("0/0"), 10, false);
        expected.addGenotype(new Genotype("0/1"), 20, false);
        expected.addGenotype(new Genotype("1/1"), 30, false);
        map.put("c1", expected);
        statsWrapper.setCohortStats(map);
        Put put = toHbase.convert(statsWrapper);

        List<Cell> cells = put.getFamilyCellMap().get(genomeHelper.getColumnFamily());

        VariantStats convert = null;
        for (Cell cell : cells) {
            byte[] value = CellUtil.cloneValue(cell);
            if (value.length > 4) {
                convert = toJava.convert(value);
            }
        }

        assertNotNull(convert);
        convert.setRefAllele("A");
        convert.setAltAllele("C");
        assertEquals(expected, convert);

    }


}