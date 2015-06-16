/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.hadoop.variant;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.protobuf.VariantStatsProtos;
import org.opencb.biodata.models.variant.protobuf.VariantStatsProtos.VariantStats.Count;
import org.opencb.biodata.models.variant.stats.VariantStats;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantStatsToHbaseConverterTest {
    
    private static VariantStats stats;
    
    @BeforeClass
    public static void setUpClass() {
        stats = new VariantStats(null, -1, null, null, 0.1, 0.01, "A", "A/A", 10, 5, -1, false, -1, -1, -1, -1);
        stats.setTransitionsCount(100);
        stats.setTransversionsCount(60);
        stats.addGenotype(new Genotype("0/0"), 100);
        stats.addGenotype(new Genotype("0/1"), 50);
        stats.addGenotype(new Genotype("1/1"), 10);
    }
    
    
    @Test(expected = UnsupportedOperationException.class)
    public void testConvertToDataModelType() {
        VariantStatsToHbaseConverter converter = new VariantStatsToHbaseConverter();
        converter.convertToDataModelType(null);
    }
    
    @Test
    public void testConvertToStorageType() {
        VariantStatsToHbaseConverter converter = new VariantStatsToHbaseConverter();
        VariantStatsProtos.VariantStats converted = converter.convertToStorageType(stats);
        
        assertEquals(stats.getMaf(), (float) converted.getMaf(), 1e-6);
        assertEquals(stats.getMgf(), (float) converted.getMgf(), 1e-6);
        assertEquals(stats.getMafAllele(), converted.getMafAllele());
        assertEquals(stats.getMgfGenotype(), converted.getMgfGenotype());
        
        assertEquals(stats.getMissingAlleles(), converted.getMissingAlleles());
        assertEquals(stats.getMissingGenotypes(), converted.getMissingGenotypes());
        
        assertEquals(stats.getTransitionsCount(), converted.getTransitionsCount());
        assertEquals(stats.getTransversionsCount(), converted.getTransversionsCount());
        
        for (Count count : converted.getGenotypesCountList()) {
            switch (count.getKey()) {
                case "0/0":
                    assertEquals(100, count.getCount());
                    break;
                case "0/1":
                    assertEquals(50, count.getCount());
                    break;
                case "1/1":
                    assertEquals(10, count.getCount());
                    break;
            }
        }
    }
    
}
