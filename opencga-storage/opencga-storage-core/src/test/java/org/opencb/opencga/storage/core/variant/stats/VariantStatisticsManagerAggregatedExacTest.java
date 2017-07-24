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

package org.opencb.opencga.storage.core.variant.stats;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import static org.junit.Assert.assertNotEquals;

/**
 * Created by hpccoll1 on 01/06/15.
 */
@Ignore
public abstract class VariantStatisticsManagerAggregatedExacTest extends VariantStatisticsManagerAggregatedTest {

    public static final String VCF_TEST_FILE_NAME = "exachead.vcf.gz";
    public static final String MAPPING_FILE = "exac-tag-mapping.properties";
    public static Properties tagMap = new Properties();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws IOException {
        tagMap.load(VariantStorageManagerTest.class.getClassLoader().getResourceAsStream(MAPPING_FILE));
    }

    @Override
    protected URI getInputUri() throws IOException {
        return getResourceUri(VCF_TEST_FILE_NAME);
    }

    @Override
    protected VariantSource.Aggregation getAggregationType() {
        return VariantSource.Aggregation.EXAC;
    }

    @Override
    protected Properties getAggregationMappingFile() {
        return tagMap;
    }

    @Override
    protected void assertValidStats(Variant variant, VariantStats variantStats) {
        assertNotEquals("Stats seem with no valid values, for instance (chr=" + variant.getChromosome()
                        + ", start=" + variant.getStart() + ", ref=" + variant.getReference() + ", alt="
                        + variant.getAlternate() + "), gtc=" + variantStats.getGenotypesCount().toString(),
                0,
                variantStats.getGenotypesCount().size());
    }
}
