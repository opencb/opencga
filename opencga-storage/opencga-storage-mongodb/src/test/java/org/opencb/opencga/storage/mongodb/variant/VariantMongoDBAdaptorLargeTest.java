package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorLargeTest;

/**
 * Created by hpccoll1 on 09/07/15.
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
