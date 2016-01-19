package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageTest;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Created by hpccoll1 on 01/06/15.
 */
public interface MongoVariantStorageManagerTestUtils extends VariantStorageTest {

    Logger logger = LoggerFactory.getLogger(MongoVariantStorageManagerTestUtils.class);

    default MongoDBVariantStorageManager getVariantStorageManager() throws Exception {
        MongoDBVariantStorageManager manager = new MongoDBVariantStorageManager();
        InputStream is = MongoVariantStorageManagerTestUtils.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
        StorageConfiguration storageConfiguration = StorageConfiguration.load(is);
        manager.setConfiguration(storageConfiguration, MongoDBVariantStorageManager.STORAGE_ENGINE_ID);
        return manager;
    }

    default void clearDB(String dbName) throws Exception {
        MongoCredentials credentials = getVariantStorageManager().getMongoCredentials(dbName);
        logger.info("Cleaning MongoDB {}", credentials.getMongoDbName());
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        MongoDataStore mongoDataStore = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        mongoManager.drop(credentials.getMongoDbName());
    }

    default int getExpectedNumLoadedVariants(VariantSource source) {
        return source.getStats().getNumRecords()
                - source.getStats().getVariantTypeCount(VariantType.SYMBOLIC)
                - source.getStats().getVariantTypeCount(VariantType.NO_VARIATION);
    }
}
