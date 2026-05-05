package org.opencb.opencga.storage.hadoop.variant.prune;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.prune.VariantPruneManagerTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

@Category(LongTests.class)
public class HadoopVariantPruneManagerTest extends VariantPruneManagerTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(),
                newOutputUri(getTestName().getMethodName()));
    }
}
