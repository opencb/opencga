package org.opencb.opencga.storage.core.variant;

import org.junit.After;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageEngine;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageTest;

@Category(ShortTests.class)
public class DummyVariantStorageEngineTest extends VariantStorageEngineTest implements DummyVariantStorageTest {
    @After
    public void tearDown() throws Exception {
        DummyVariantStorageEngine.clear();
//        DummyVariantStorageEngine.writeAndClear(Paths.get(newOutputUri()));
    }
}
