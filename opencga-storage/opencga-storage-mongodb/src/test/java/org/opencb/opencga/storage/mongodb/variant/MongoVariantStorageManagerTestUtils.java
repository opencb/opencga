package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Created by hpccoll1 on 01/06/15.
 */
public class MongoVariantStorageManagerTestUtils {

    static public MongoDBVariantStorageManager manager = null;
    public static Logger logger = LoggerFactory.getLogger(MongoVariantStorageManagerTestUtils.class);;

    static public MongoDBVariantStorageManager getVariantStorageManager() throws Exception {
        if (manager == null) {
            manager = new MongoDBVariantStorageManager();
            InputStream is = MongoVariantStorageManagerTestUtils.class.getClassLoader().getResourceAsStream("configuration.yml");
            StorageConfiguration storageConfiguration = StorageConfiguration.load(is);
            manager.setConfiguration(storageConfiguration, "mongodb");
        }
        return manager;
    }

    static public void clearDB(String dbName) throws Exception {
        MongoCredentials credentials = getVariantStorageManager().getMongoCredentials(dbName);
        logger.info("Cleaning MongoDB {}" , credentials.getMongoDbName());
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        MongoDataStore mongoDataStore = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        mongoManager.drop(credentials.getMongoDbName());
    }
}
