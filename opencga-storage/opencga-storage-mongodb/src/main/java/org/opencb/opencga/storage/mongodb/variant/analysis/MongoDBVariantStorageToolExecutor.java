package org.opencb.opencga.storage.mongodb.variant.analysis;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.exception.ToolExecutorException;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;

/**
 * Helper interface to be used by opencga mongodb analysis executors.
 */
public interface MongoDBVariantStorageToolExecutor {

    ObjectMap getExecutorParams();

    default MongoDBVariantStorageEngine getMongoDBVariantStorageEngine() throws ToolExecutorException {
        ObjectMap executorParams = getExecutorParams();
        String storageEngine = executorParams.getString("storageEngineId");
        if (StringUtils.isEmpty(storageEngine)) {
            throw new ToolExecutorException("Missing arguments!");
        } else {
            if (!storageEngine.equals(MongoDBVariantStorageEngine.STORAGE_ENGINE_ID)) {
                throw new ToolExecutorException("Unable to use executor '" + getClass() + "' with storageEngine " + storageEngine);
            }
            String dbName = executorParams.getString("dbName");
            try {
                return (MongoDBVariantStorageEngine) StorageEngineFactory.get()
                        .getVariantStorageEngine(MongoDBVariantStorageEngine.STORAGE_ENGINE_ID, dbName);
            } catch (StorageEngineException e) {
                throw new ToolExecutorException(e);
            }

        }

    }
}
