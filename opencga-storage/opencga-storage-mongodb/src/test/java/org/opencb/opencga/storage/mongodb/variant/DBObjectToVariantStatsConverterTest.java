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

package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.stats.VariantStats;

import static org.junit.Assert.assertEquals;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantStatsConverterTest {

    private static BasicDBObject mongoStats;
    private static VariantStats stats;

    @BeforeClass
    public static void setUpClass() {
        mongoStats = new BasicDBObject(DBObjectToVariantStatsConverter.MAF_FIELD, 0.1);
        mongoStats.append(DBObjectToVariantStatsConverter.MGF_FIELD, 0.01);
        mongoStats.append(DBObjectToVariantStatsConverter.MAFALLELE_FIELD, "A");
        mongoStats.append(DBObjectToVariantStatsConverter.MGFGENOTYPE_FIELD, "A/A");
        mongoStats.append(DBObjectToVariantStatsConverter.MISSALLELE_FIELD, 10);
        mongoStats.append(DBObjectToVariantStatsConverter.MISSGENOTYPE_FIELD, 5);

        BasicDBObject genotypes = new BasicDBObject();
        genotypes.append("0/0", 100);
        genotypes.append("0/1", 50);
        genotypes.append("1/1", 10);
        mongoStats.append(DBObjectToVariantStatsConverter.NUMGT_FIELD, genotypes);

        stats = new VariantStats(null, -1, null, null, VariantType.SNV, 0.1f, 0.01f, "A", "A/A", 10, 5, -1, -1, -1, -1, -1);
        stats.addGenotype(new Genotype("0/0"), 100);
        stats.addGenotype(new Genotype("0/1"), 50);
        stats.addGenotype(new Genotype("1/1"), 10);
    }

    @Test
    public void testConvertToDataModelType() {
        DBObjectToVariantStatsConverter converter = new DBObjectToVariantStatsConverter();
        VariantStats converted = converter.convertToDataModelType(mongoStats);
        assertEquals(stats, converted);
    }

    @Test
    public void testConvertToStorageType() {
        DBObjectToVariantStatsConverter converter = new DBObjectToVariantStatsConverter();
        DBObject converted = converter.convertToStorageType(stats);

        assertEquals(stats.getMaf(), (float) converted.get(DBObjectToVariantStatsConverter.MAF_FIELD), 1e-6);
        assertEquals(stats.getMgf(), (float) converted.get(DBObjectToVariantStatsConverter.MGF_FIELD), 1e-6);
        assertEquals(stats.getMafAllele(), converted.get(DBObjectToVariantStatsConverter.MAFALLELE_FIELD));
        assertEquals(stats.getMgfGenotype(), converted.get(DBObjectToVariantStatsConverter.MGFGENOTYPE_FIELD));

        assertEquals(stats.getMissingAlleles(), converted.get(DBObjectToVariantStatsConverter.MISSALLELE_FIELD));
        assertEquals(stats.getMissingGenotypes(), converted.get(DBObjectToVariantStatsConverter.MISSGENOTYPE_FIELD));

        assertEquals(100, ((DBObject) converted.get(DBObjectToVariantStatsConverter.NUMGT_FIELD)).get("0/0"));
        assertEquals(50, ((DBObject) converted.get(DBObjectToVariantStatsConverter.NUMGT_FIELD)).get("0/1"));
        assertEquals(10, ((DBObject) converted.get(DBObjectToVariantStatsConverter.NUMGT_FIELD)).get("1/1"));
    }
}
