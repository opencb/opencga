package org.opencb.opencga.storage.hadoop.variant;

import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineDuplicatedTest;

@Category(MediumTests.class)
public class HadoopVariantStorageEngineDuplicatedTest extends VariantStorageEngineDuplicatedTest implements HadoopVariantStorageTest {

    @Rule
    public HadoopExternalResource externalResource = new HadoopExternalResource();

    @Override
    public void testDuplicatedVariant() throws Exception {
        super.testDuplicatedVariant();

        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }
}
