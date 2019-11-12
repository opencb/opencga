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

package org.opencb.opencga.storage.hadoop.variant.stats;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManagerTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;

/**
 * Created on 12/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStatisticsManagerTest extends VariantStatisticsManagerTest implements HadoopVariantStorageTest {

    @Override
    public void before() throws Exception {
        super.before();
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }

    @Rule
    public ExternalResource externalResource = new HadoopExternalResource();

    @Override
    public Map<String, ?> getOtherStorageConfigurationOptions() {
        return new ObjectMap(HadoopVariantStorageOptions.VARIANT_TABLE_INDEXES_SKIP.key(), true)
                .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
                .append(HadoopVariantStorageOptions.STATS_LOCAL.key(), false);
    }

    @Test
    public void testStatsToFile() throws Exception {

        String cohortName = "MyCohort";
        metadataManager.registerCohort(studyMetadata.getName(), cohortName, Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685"));

        VariantHadoopDBAdaptor dbAdaptor = (VariantHadoopDBAdaptor) this.dbAdaptor;
        ObjectMap options = new ObjectMap();
        options.put(VariantStatsDriver.COHORTS, cohortName);
        URI outputFile = newOutputUri().resolve(cohortName + ".tsv");
        options.put(VariantStatsDriver.OUTPUT, outputFile);

        getMrExecutor().run(VariantStatsDriver.class, VariantStatsDriver.buildArgs(
                dbAdaptor.getArchiveTableName(studyMetadata.getId()),
                dbAdaptor.getVariantTable(), studyMetadata.getId(), null, options), options);

        try(BufferedReader is = new BufferedReader(new FileReader(outputFile.getPath()))) {
            long count = is.lines().count();
            int headerSize = 1;
            Assert.assertEquals(dbAdaptor.count(new Query()).first() + headerSize, count);
        }

    }
}
