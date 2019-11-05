package org.opencb.opencga.storage.hadoop.variant.stats;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManagerMultiFilesTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngineOptions;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.util.Map;

/**
 * Created on 14/03/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStatisticsManagerMultiFilesTest extends VariantStatisticsManagerMultiFilesTest implements HadoopVariantStorageTest {

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
    public ExternalResource externalResource = new HadoopVariantStorageTest.HadoopExternalResource();

    @Override
    public Map<String, ?> getOtherStorageConfigurationOptions() {
        return new ObjectMap(HadoopVariantStorageEngineOptions.VARIANT_TABLE_INDEXES_SKIP.key(), true)
                .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
                .append(HadoopVariantStorageEngineOptions.STATS_LOCAL.key(), false)
                .append(HadoopVariantStorageEngineOptions.HADOOP_LOAD_BATCH_SIZE.key(), 1);
    }

    @Test
    public void calculateStatsMultiCohortsAfterFillMissingTest() throws Exception {
        VariantStorageEngine storageEngine = getVariantStorageEngine();
        storageEngine.fillMissing(studyMetadata.getName(), new ObjectMap(), false);
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
        calculateStatsMultiCohortsTest();
    }


}
