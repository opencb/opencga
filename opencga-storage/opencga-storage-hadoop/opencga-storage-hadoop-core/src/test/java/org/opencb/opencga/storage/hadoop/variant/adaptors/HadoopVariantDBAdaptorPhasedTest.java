package org.opencb.opencga.storage.hadoop.variant.adaptors;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorPhasedTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

@Ignore // Ignored until phased variants are supported in Hadoop storage engine
public class HadoopVariantDBAdaptorPhasedTest extends VariantDBAdaptorPhasedTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

}
