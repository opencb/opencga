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

package org.opencb.opencga.storage.mongodb.variant.converters;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
@Category(ShortTests.class)
public class DocumentToVariantStatsConverterTest {

    private Document mongoStats;
    private VariantStats stats;

    @Before
    public void setUpClass() {
        float numSamples = 160;
        float numAlleles = 320;

        mongoStats = new Document();
        mongoStats.append(DocumentToVariantStatsConverter.REF_FREQ_FIELD, 250 / numAlleles);
        mongoStats.append(DocumentToVariantStatsConverter.ALT_FREQ_FIELD, 70 / numAlleles);
        mongoStats.append(DocumentToVariantStatsConverter.MAF_FIELD, 70 / numAlleles);
        mongoStats.append(DocumentToVariantStatsConverter.MGF_FIELD, 0.01);
        mongoStats.append(DocumentToVariantStatsConverter.MAFALLELE_FIELD, "A");
        mongoStats.append(DocumentToVariantStatsConverter.MGFGENOTYPE_FIELD, "A/A");
        mongoStats.append(DocumentToVariantStatsConverter.MISSALLELE_FIELD, 10);
        mongoStats.append(DocumentToVariantStatsConverter.MISSGENOTYPE_FIELD, 5);

        Document genotypes = new Document();
        genotypes.append("0/0", 100);
        genotypes.append("0/1", 50);
        genotypes.append("1/1", 10);
        mongoStats.append(DocumentToVariantStatsConverter.GENOTYPE_COUNT_FIELD, genotypes);

//        stats = new VariantStats(null, -1, null, null, VariantType.SNV, 0.1f, 0.01f, "A", "A/A", 10, 5, -1, -1, -1, -1, -1);
        stats = new VariantStats(70 / numAlleles, 0.01f, "A", "A/A", 10, 5);
        stats.addGenotype("0/0", 100);
        stats.addGenotype("0/1", 50);
        stats.addGenotype("1/1", 10);
        Map<String, Float> genotypeFreq = new HashMap<>();

        genotypeFreq.put("0/0", 100 / numSamples);
        genotypeFreq.put("0/1", 50 / numSamples);
        genotypeFreq.put("1/1", 10 / numSamples);
        stats.setAlleleCount((int) numAlleles);
        stats.setGenotypeFreq(genotypeFreq);
        stats.setRefAlleleCount(250);
        stats.setRefAlleleFreq(250 / numAlleles);
        stats.setAltAlleleCount(70);
        stats.setAltAlleleFreq(70 / numAlleles);

    }

    @Test
    public void testConvertToDataModelType() {
        DocumentToVariantStatsConverter converter = new DocumentToVariantStatsConverter();
        VariantStats converted = converter.convertToDataModelType(mongoStats);
        assertEquals(stats, converted);
    }

    @Test
    public void testConvertToDataModelTypeWithoutAF() {
        DocumentToVariantStatsConverter converter = new DocumentToVariantStatsConverter();
        mongoStats.remove(DocumentToVariantStatsConverter.ALT_FREQ_FIELD);
        mongoStats.remove(DocumentToVariantStatsConverter.REF_FREQ_FIELD);
        VariantStats converted = converter.convertToDataModelType(mongoStats);
        assertEquals(stats, converted);
    }

    @Test
    public void testConvertToDataModelTypeNegativeAF() {
        DocumentToVariantStatsConverter converter = new DocumentToVariantStatsConverter();
        mongoStats.put(DocumentToVariantStatsConverter.ALT_FREQ_FIELD, -1);
        mongoStats.put(DocumentToVariantStatsConverter.REF_FREQ_FIELD, -1);
        VariantStats converted = converter.convertToDataModelType(mongoStats);
        assertEquals(stats, converted);
    }

    @Test
    public void testConvertToDataModelTypeWithoutGtCount() {
        stats.getGenotypeCount().clear();
        stats.getGenotypeFreq().clear();
        stats.setAlleleCount(-1);
        stats.setRefAlleleCount(-1);
        stats.setAltAlleleCount(-1);
        DocumentToVariantStatsConverter converter = new DocumentToVariantStatsConverter();
        mongoStats.put(DocumentToVariantStatsConverter.GENOTYPE_COUNT_FIELD, new Document());
        VariantStats converted = converter.convertToDataModelType(mongoStats);
        assertEquals(stats, converted);
    }

    @Test
    public void testConvertToDataModelTypeWithoutGtCountAndAF() {
        stats.getGenotypeCount().clear();
        stats.getGenotypeFreq().clear();
        stats.setAlleleCount(-1);
        stats.setRefAlleleCount(-1);
        stats.setAltAlleleCount(-1);
        DocumentToVariantStatsConverter converter = new DocumentToVariantStatsConverter();
        mongoStats.put(DocumentToVariantStatsConverter.GENOTYPE_COUNT_FIELD, new Document());
        mongoStats.remove(DocumentToVariantStatsConverter.ALT_FREQ_FIELD);
        mongoStats.remove(DocumentToVariantStatsConverter.REF_FREQ_FIELD);
        VariantStats converted = converter.convertToDataModelType(mongoStats, new Variant("1", 100, "C", "A"));
        assertEquals(stats, converted);
    }

    @Test
    public void testConvertToStorageType() {
        DocumentToVariantStatsConverter converter = new DocumentToVariantStatsConverter();
        Document converted = converter.convertToStorageType(stats);

        assertEquals(stats.getMaf(), (float) converted.get(DocumentToVariantStatsConverter.MAF_FIELD), 1e-6);
        assertEquals(stats.getMgf(), (float) converted.get(DocumentToVariantStatsConverter.MGF_FIELD), 1e-6);
        assertEquals(stats.getMafAllele(), converted.get(DocumentToVariantStatsConverter.MAFALLELE_FIELD));
        assertEquals(stats.getMgfGenotype(), converted.get(DocumentToVariantStatsConverter.MGFGENOTYPE_FIELD));

        assertEquals(stats.getMissingAlleleCount(), converted.get(DocumentToVariantStatsConverter.MISSALLELE_FIELD));
        assertEquals(stats.getMissingGenotypeCount(), converted.get(DocumentToVariantStatsConverter.MISSGENOTYPE_FIELD));

        assertEquals(100, (converted.get(DocumentToVariantStatsConverter.GENOTYPE_COUNT_FIELD, Document.class)).get("0/0"));
        assertEquals(50, (converted.get(DocumentToVariantStatsConverter.GENOTYPE_COUNT_FIELD, Document.class)).get("0/1"));
        assertEquals(10, (converted.get(DocumentToVariantStatsConverter.GENOTYPE_COUNT_FIELD, Document.class)).get("1/1"));
    }
}
