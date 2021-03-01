package org.opencb.opencga.clinical;

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.cache.CacheManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StorageManager {

    protected final CatalogManager catalogManager;
    protected final CacheManager cacheManager;
    protected final StorageConfiguration storageConfiguration;
    protected final StorageEngineFactory storageEngineFactory;

    protected final Logger logger;

    public StorageManager(Configuration configuration, StorageConfiguration storageConfiguration) throws CatalogException {
        this(new CatalogManager(configuration), StorageEngineFactory.get(storageConfiguration));
    }

    public StorageManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        this(catalogManager, null, storageEngineFactory.getStorageConfiguration(), storageEngineFactory);
    }

    protected StorageManager(CatalogManager catalogManager, CacheManager cacheManager, StorageConfiguration storageConfiguration,
                             StorageEngineFactory storageEngineFactory) {
        this.catalogManager = catalogManager;
        this.cacheManager = cacheManager == null ? new CacheManager(storageConfiguration) : cacheManager;
        this.storageConfiguration = storageConfiguration;
        this.storageEngineFactory = storageEngineFactory == null
                ? StorageEngineFactory.get(storageConfiguration)
                : storageEngineFactory;
        logger = LoggerFactory.getLogger(getClass());
    }


    public void clearCache(String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);

    }


    public void clearCache(String studyId, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);

    }

    public abstract void testConnection() throws StorageEngineException;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StorageEngine{");
        sb.append("catalogManager=").append(catalogManager);
        sb.append(", cacheManager=").append(cacheManager);
        sb.append(", storageConfiguration=").append(storageConfiguration);
        sb.append('}');
        return sb.toString();
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    public StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }
}
