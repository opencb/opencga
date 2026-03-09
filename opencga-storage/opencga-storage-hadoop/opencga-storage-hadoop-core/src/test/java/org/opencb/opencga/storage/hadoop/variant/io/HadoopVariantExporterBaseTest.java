package org.opencb.opencga.storage.hadoop.variant.io;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.io.VariantExporterTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

/**
 * Runs the generic VariantExporterTest suite (including exportJsonSparseTest) against Hadoop.
 */
@Category(MediumTests.class)
public class HadoopVariantExporterBaseTest extends VariantExporterTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    private static boolean loaded = false;

    @Override
    @Before
    public void setUp() throws Exception {
        if (!loaded) {
            super.setUp();
            loaded = true;
        }
    }
}
