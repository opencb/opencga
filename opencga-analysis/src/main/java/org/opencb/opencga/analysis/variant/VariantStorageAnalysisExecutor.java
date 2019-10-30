package org.opencb.opencga.analysis.variant;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.ConfigurationUtils;
import org.opencb.opencga.core.exception.AnalysisExecutorException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.analysis.storage.variant.VariantStorageManager;

import java.io.IOException;

/**
 * Helper interface to be used by opencga local analysis executors.
 */
public interface VariantStorageAnalysisExecutor {

    ObjectMap getExecutorParams();


    default VariantStorageManager getVariantStorageManager() throws AnalysisExecutorException {
        String opencgaHome = getExecutorParams().getString("opencgaHome");
        try {
            Configuration configuration = ConfigurationUtils.loadConfiguration(opencgaHome);
            StorageConfiguration storageConfiguration = ConfigurationUtils.loadStorageConfiguration(opencgaHome);

            CatalogManager catalogManager = new CatalogManager(configuration);
            StorageEngineFactory engineFactory = StorageEngineFactory.get(storageConfiguration);
            return new VariantStorageManager(catalogManager, engineFactory);
        } catch (CatalogException | IOException e) {
            throw new AnalysisExecutorException(e);
        }
    }

}
