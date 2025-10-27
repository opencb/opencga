package org.opencb.opencga.storage.hadoop.variant;

import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineTest;

@Category(LongTests.class)
public class HadoopVariantStorageEngineTest extends VariantStorageEngineTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

}
