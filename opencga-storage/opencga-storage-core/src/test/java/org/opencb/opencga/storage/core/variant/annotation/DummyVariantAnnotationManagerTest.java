package org.opencb.opencga.storage.core.variant.annotation;

import org.junit.Assume;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageTest;


@Category(ShortTests.class)
public class DummyVariantAnnotationManagerTest extends VariantAnnotationManagerTest implements DummyVariantStorageTest {

    @Override
    public void testMultiAnnotations() throws Exception {
        Assume.assumeTrue("Multi annotations not supported in DummyVariantStorageEngine", false);
        super.testMultiAnnotations();
    }

    @Override
    public void testCheckpointAnnotation() throws Exception {
        Assume.assumeTrue("Can not check actual results", false);
        super.testCheckpointAnnotation();
    }

    @Override
    public void testCosmicAnnotatorExtensionWithCosmicAnnotation() throws Exception {
        Assume.assumeTrue("Can not check actual results", false);
        super.testCosmicAnnotatorExtensionWithCosmicAnnotation();
    }
}
