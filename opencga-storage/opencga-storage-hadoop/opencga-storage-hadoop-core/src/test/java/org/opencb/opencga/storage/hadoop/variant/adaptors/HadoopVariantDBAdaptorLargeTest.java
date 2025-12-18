package org.opencb.opencga.storage.hadoop.variant.adaptors;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorLargeTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

@Ignore // This test is too large. Is it really needed?
@Category(LongTests.class)
public class HadoopVariantDBAdaptorLargeTest extends VariantDBAdaptorLargeTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

}
