package org.opencb.opencga.storage.hadoop.variant.adaptors;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorLargeTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

@Category(LongTests.class)
public class HadoopVariantDBAdaptorLargeTest extends VariantDBAdaptorLargeTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    @Test
    @Ignore("Hadoop backend does not distinguish SNP from SNV — all SNPs are reported as SNV.")
    @Override
    public void testGetVariantsByType() {
    }

}
