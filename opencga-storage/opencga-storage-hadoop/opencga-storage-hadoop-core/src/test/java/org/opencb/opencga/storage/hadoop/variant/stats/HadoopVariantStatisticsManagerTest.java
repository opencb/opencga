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

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.DummyTestAnnotator;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManagerTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.analysis.julie.JulieToolDriver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    @Test
    public void testJulieTool() throws Exception {
        VariantHadoopDBAdaptor dbAdaptor = (VariantHadoopDBAdaptor) this.dbAdaptor;
        HadoopVariantStorageEngine engine = (HadoopVariantStorageEngine) this.variantStorageEngine;

        engine.getOptions().put(VariantStorageOptions.ANNOTATOR.key(), "other");
        engine.getOptions().put(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyTestAnnotator.class.getName());
        engine.annotate(new Query(), new QueryOptions(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));

        engine.getMRExecutor().run(JulieToolDriver.class, JulieToolDriver.buildArgs(
                dbAdaptor.getVariantTable(),
                new QueryOptions()
        ), engine.getOptions(), "Execute Julie Tool");

        for (Variant variant : dbAdaptor) {
            List<String> expected = variant.getStudies()
                    .stream()
                    .flatMap(s -> s.getStats()
                            .stream()
                            .filter(c -> c.getAltAlleleFreq() > 0)
                            .map(c -> s.getStudyId() + ":" + c.getCohortId()))
                    .collect(Collectors.toList());
            List<PopulationFrequency> populationFrequencies = variant.getAnnotation().getPopulationFrequencies();
            if (populationFrequencies == null) {
                populationFrequencies = Collections.emptyList();
            }
            Assert.assertEquals(expected.size(), populationFrequencies.size());
            for (PopulationFrequency populationFrequency : populationFrequencies) {
                VariantStats stats = variant.getStudy(populationFrequency.getStudy()).getStats(populationFrequency.getPopulation());
                Assert.assertNotNull(stats);
                Assert.assertThat(expected, CoreMatchers.hasItem(populationFrequency.getStudy() + ":" + populationFrequency.getPopulation()));
                Assert.assertEquals(stats.getAltAlleleFreq(), populationFrequency.getAltAlleleFreq());
                Assert.assertEquals(stats.getRefAlleleFreq(), populationFrequency.getRefAlleleFreq());
            }
        }
    }
}
