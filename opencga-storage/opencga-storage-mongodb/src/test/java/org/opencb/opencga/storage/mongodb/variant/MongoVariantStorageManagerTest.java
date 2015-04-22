package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;


/**
 * Created by hpccoll1 on 13/03/15.
 */
public class MongoVariantStorageManagerTest extends VariantStorageManagerTest {

    static protected MongoDBVariantStorageManager manager = null;
    @Override
    protected MongoDBVariantStorageManager getVariantStorageManager() throws Exception {
        if (manager == null) {
            manager = new MongoDBVariantStorageManager();
            manager.addConfigUri(this.getClass().getClassLoader().getResource("storage-mongo-test.properties").toURI());
        }
        return manager;
    }

    @Override
    protected void clearDB() throws Exception {
        MongoCredentials credentials = getVariantStorageManager().getMongoCredentials();
        logger.info("Cleaning MongoDB {}" , credentials.getMongoDbName());
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        MongoDataStore mongoDataStore = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        mongoManager.drop(credentials.getMongoDbName());
    }

}
