package org.opencb.opencga.storage.mongodb.variant.io;

import org.junit.After;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.io.VariantExporterTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

@Category(MediumTests.class)
public class MongoDBVariantExporterTest extends VariantExporterTest implements MongoDBVariantStorageTest {

    @After
    public void tearDown() throws Exception {
        closeConnections();
    }
}
