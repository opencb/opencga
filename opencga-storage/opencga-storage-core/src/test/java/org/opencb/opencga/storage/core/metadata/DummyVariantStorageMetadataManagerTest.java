package org.opencb.opencga.storage.core.metadata;

import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;


@Category(ShortTests.class)
public class DummyVariantStorageMetadataManagerTest extends VariantStorageMetadataManagerTest {

    @Override
    public void setUp() throws Exception {
        DummyVariantStorageMetadataDBAdaptorFactory.clear();
        super.setUp();
    }

    @Override
    protected VariantStorageMetadataManager getMetadataManager() throws Exception {
        return new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
    }

}
