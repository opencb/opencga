package org.opencb.opencga.storage.hadoop.variant.stats;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManagerMultiFilesTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
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
        return new ObjectMap(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, true)
                .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
                .append(HadoopVariantStorageEngine.STATS_LOCAL, false)
                .append(HadoopVariantStorageEngine.HADOOP_LOAD_ARCHIVE_BATCH_SIZE, 1);
    }

    @Test
    public void calculateStatsMultiCohortsAfterFillMissingTest() throws Exception {
        VariantStorageEngine storageEngine = getVariantStorageEngine();
        storageEngine.fillMissing(studyConfiguration.getStudyName(), new ObjectMap(), false);
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
        calculateStatsMultiCohortsTest();
    }


}
