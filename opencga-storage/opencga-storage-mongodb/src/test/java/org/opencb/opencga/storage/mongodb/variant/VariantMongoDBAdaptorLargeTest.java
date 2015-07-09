package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorLargeTest;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMongoDBAdaptorLargeTest extends VariantDBAdaptorLargeTest {

    @Override
    protected MongoDBVariantStorageManager getVariantStorageManager() throws Exception {
        return MongoVariantStorageManagerTestUtils.getVariantStorageManager();
    }

    @Override
    protected void clearDB(String dbName) throws Exception {
        MongoVariantStorageManagerTestUtils.clearDB(dbName);
    }
}
