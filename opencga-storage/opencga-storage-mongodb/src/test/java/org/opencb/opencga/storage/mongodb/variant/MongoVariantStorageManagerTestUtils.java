package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hpccoll1 on 01/06/15.
 */
public class MongoVariantStorageManagerTestUtils {

    static public MongoDBVariantStorageManager manager = null;
    public static Logger logger = LoggerFactory.getLogger(MongoVariantStorageManagerTestUtils.class);;

    static public MongoDBVariantStorageManager getVariantStorageManager() throws Exception {
        if (manager == null) {
            manager = new MongoDBVariantStorageManager();
            manager.addConfigUri(MongoVariantStorageManagerTestUtils.class.getClassLoader().getResource("storage-mongo-test.properties").toURI());
        }
        return manager;
    }

    static public void clearDB() throws Exception {
        MongoCredentials credentials = getVariantStorageManager().getMongoCredentials();
        logger.info("Cleaning MongoDB {}" , credentials.getMongoDbName());
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        MongoDataStore mongoDataStore = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        mongoManager.drop(credentials.getMongoDbName());
    }
}
