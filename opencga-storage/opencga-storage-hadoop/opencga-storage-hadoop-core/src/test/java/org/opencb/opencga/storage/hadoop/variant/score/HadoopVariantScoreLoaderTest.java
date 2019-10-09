package org.opencb.opencga.storage.hadoop.variant.score;

import org.junit.After;
import org.junit.ClassRule;
import org.opencb.opencga.storage.core.variant.score.VariantScoreLoaderTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

public class HadoopVariantScoreLoaderTest extends VariantScoreLoaderTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }
}