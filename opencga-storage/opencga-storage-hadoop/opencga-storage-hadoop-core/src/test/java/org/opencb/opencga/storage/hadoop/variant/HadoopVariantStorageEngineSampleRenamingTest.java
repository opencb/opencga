package org.opencb.opencga.storage.hadoop.variant;

import org.junit.Rule;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineSampleRenamingTest;

public class HadoopVariantStorageEngineSampleRenamingTest extends VariantStorageEngineSampleRenamingTest implements HadoopVariantStorageTest {

    @Rule
    public HadoopExternalResource externalResource = new HadoopExternalResource();

}
