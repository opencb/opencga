package org.opencb.opencga.storage.hadoop.variant;

import org.junit.Rule;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineDuplicatedTest;

public class HadoopVariantStorageEngineDuplicatedTest extends VariantStorageEngineDuplicatedTest implements HadoopVariantStorageTest {

    @Rule
    public HadoopExternalResource externalResource = new HadoopExternalResource();

    @Override
    public void testDuplicatedVariant() throws Exception {
        super.testDuplicatedVariant();

        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }
}
