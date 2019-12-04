package org.opencb.opencga.storage.hadoop.variant.stats;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManagerMultiFilesTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
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
        return new ObjectMap(HadoopVariantStorageOptions.VARIANT_TABLE_INDEXES_SKIP.key(), true)
                .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
                .append(HadoopVariantStorageOptions.STATS_LOCAL.key(), false)
                .append(HadoopVariantStorageOptions.HADOOP_LOAD_FILES_IN_PARALLEL.key(), 1);
    }

    @Test
    public void calculateStatsMultiCohortsAfterFillMissingTest() throws Exception {
        VariantStorageEngine storageEngine = getVariantStorageEngine();
        storageEngine.aggregate(studyMetadata.getName(), new ObjectMap(), false);
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
        calculateStatsMultiCohortsTest();
    }


}
