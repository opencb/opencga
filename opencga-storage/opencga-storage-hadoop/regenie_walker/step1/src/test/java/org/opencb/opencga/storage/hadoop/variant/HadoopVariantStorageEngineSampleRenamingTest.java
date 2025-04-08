package org.opencb.opencga.storage.hadoop.variant;

import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineSampleRenamingTest;

@Category(MediumTests.class)
public class HadoopVariantStorageEngineSampleRenamingTest extends VariantStorageEngineSampleRenamingTest implements HadoopVariantStorageTest {

    @Rule
    public HadoopExternalResource externalResource = new HadoopExternalResource();

}
