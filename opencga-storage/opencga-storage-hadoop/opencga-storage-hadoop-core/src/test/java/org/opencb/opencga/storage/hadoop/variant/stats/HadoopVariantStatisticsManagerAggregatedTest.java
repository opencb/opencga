package org.opencb.opencga.storage.hadoop.variant.stats;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManagerAggregatedTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.util.Map;

/**
 * Created on 11/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStatisticsManagerAggregatedTest extends VariantStatisticsManagerAggregatedTest implements HadoopVariantStorageTest {

    @Rule
    public ExternalResource externalResource = new HadoopExternalResource();

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(studyMetadata, ((VariantHadoopDBAdaptor) dbAdaptor), newOutputUri(getTestName().getMethodName()));
    }

    @Override
    public Map<String, ?> getOtherStorageConfigurationOptions() {
        return new ObjectMap(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, true)
                .append(HadoopVariantStorageEngine.STATS_LOCAL, false)
                .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
    }
}