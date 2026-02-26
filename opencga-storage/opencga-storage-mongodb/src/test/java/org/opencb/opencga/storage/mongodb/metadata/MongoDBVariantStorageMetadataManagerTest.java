package org.opencb.opencga.storage.mongodb.metadata;

import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManagerTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.DB_NAME;

@Category(MediumTests.class)
public class MongoDBVariantStorageMetadataManagerTest extends VariantStorageMetadataManagerTest implements MongoDBVariantStorageTest {

    private MongoDBVariantStorageEngine engine;

    @Before
    @Override
    public void setUp() throws Exception {
        clearDB(DB_NAME);
        super.setUp();
    }

    @Override
    protected VariantStorageMetadataManager getMetadataManager() throws Exception {
        if (engine == null) {
            engine = newVariantStorageEngine();
        }
        return engine.getMetadataManager();
    }
}
