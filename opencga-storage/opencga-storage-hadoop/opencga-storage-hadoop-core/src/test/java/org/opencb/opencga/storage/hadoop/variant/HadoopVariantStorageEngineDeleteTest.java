package org.opencb.opencga.storage.hadoop.variant;

import org.junit.After;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.VariantStorageEngineDeleteTest;

@Category(LongTests.class)
public class HadoopVariantStorageEngineDeleteTest extends VariantStorageEngineDeleteTest implements HadoopVariantStorageTest {

    @Rule
    public ExternalResource externalResource = new HadoopExternalResource();

    @Override
    public void before() throws Exception {
        super.before();
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }


}
