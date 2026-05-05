package org.opencb.opencga.storage.mongodb.variant.walker;

import org.junit.After;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.walker.VariantWalkerTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

@Category(MediumTests.class)
public class MongoDBVariantWalkerTest extends VariantWalkerTest implements MongoDBVariantStorageTest {

    @After
    public void tearDown() throws Exception {
        closeConnections();
    }
}
