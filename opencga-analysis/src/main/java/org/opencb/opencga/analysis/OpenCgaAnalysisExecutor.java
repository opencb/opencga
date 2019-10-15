package org.opencb.opencga.analysis;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.oskar.analysis.exceptions.AnalysisExecutorException;

import java.io.IOException;

public interface OpenCgaAnalysisExecutor {

    ObjectMap getExecutorParams();

    default String getSessionId() throws AnalysisExecutorException {
        return getExecutorParams().getString("sessionId");
    }

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
