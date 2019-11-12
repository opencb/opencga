package org.opencb.opencga.storage.hadoop.variant.analysis;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.exception.AnalysisExecutorException;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;

/**
 * Helper interface to be used by opencga hadoop analysis executors.
 */
public interface HadoopVariantAnalysisExecutor {

    ObjectMap getExecutorParams();

    default HadoopVariantStorageEngine getHadoopVariantStorageEngine() throws AnalysisExecutorException {
        ObjectMap executorParams = getExecutorParams();
        String storageEngine = executorParams.getString("storageEngineId");
        if (StringUtils.isEmpty(storageEngine)) {
            throw new AnalysisExecutorException("Missing arguments!");
        } else {
            if (!storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
                throw new AnalysisExecutorException("Unable to use executor '" + getClass() + "' with storageEngine " + storageEngine);
            }
            String dbName = executorParams.getString("dbName");
            try {
                return (HadoopVariantStorageEngine) StorageEngineFactory.get()
                        .getVariantStorageEngine(HadoopVariantStorageEngine.STORAGE_ENGINE_ID, dbName);
            } catch (StorageEngineException e) {
                throw new AnalysisExecutorException(e);
            }

        }
    }

}
