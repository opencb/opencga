package org.opencb.opencga.storage.hadoop.variant.annotation;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.annotation.CustomVariantAnnotationManagerTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

/**
 * Exists only to satisfy {@code StorageEngineTestRegistry}, which requires every
 * {@code @StorageEngineTest}-annotated abstract test in core to have at least one
 * backend implementation.
 *
 * <p>Custom variant annotation loading (BED/GFF/VCF) is not implemented on the Hadoop
 * backend: {@code VariantHadoopDBAdaptor.updateCustomAnnotations} throws
 * {@link UnsupportedOperationException}. Until that is implemented, this suite is
 * ignored.</p>
 */
@Ignore("Custom variant annotations are not implemented on the Hadoop backend "
        + "(VariantHadoopDBAdaptor.updateCustomAnnotations is unimplemented).")
@Category(LongTests.class)
public class HadoopVariantCustomAnnotationManagerTest  extends CustomVariantAnnotationManagerTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

}