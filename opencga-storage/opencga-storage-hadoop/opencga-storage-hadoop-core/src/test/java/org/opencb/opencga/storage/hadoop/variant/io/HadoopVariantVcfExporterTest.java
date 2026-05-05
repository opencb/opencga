package org.opencb.opencga.storage.hadoop.variant.io;

import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.io.VariantVcfExporterTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

/**
 * Hadoop backend implementation of {@link VariantVcfExporterTest}. The abstract
 * parent is {@code @Ignore}d, so this subclass exists primarily to satisfy
 * {@code StorageEngineTestRegistry}, which requires every {@code @StorageEngineTest}
 * abstract test in core to have a backend implementation.
 */
@Category(LongTests.class)
public class HadoopVariantVcfExporterTest extends VariantVcfExporterTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

}