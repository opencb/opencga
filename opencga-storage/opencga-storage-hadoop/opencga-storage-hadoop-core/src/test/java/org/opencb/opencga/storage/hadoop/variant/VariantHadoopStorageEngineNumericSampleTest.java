package org.opencb.opencga.storage.hadoop.variant;

import org.junit.After;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineNumericSampleTest;

@Category(MediumTests.class)
public class VariantHadoopStorageEngineNumericSampleTest extends VariantStorageEngineNumericSampleTest implements HadoopVariantStorageTest {

    @Rule
    public HadoopExternalResource externalResource = new HadoopExternalResource();

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }
}
