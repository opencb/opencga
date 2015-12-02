package org.opencb.opencga.analysis.execution.plugins;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.beans.Analysis;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;

/**
 * Created on 26/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class OpenCGAPlugin {

    private Logger logger;
    private ObjectMap configuration;
    private CatalogManager catalogManager;
    private String sessionId;
    private boolean initialized;
    private StorageManagerFactory storageManagerFactory;
    private int studyId;

    public abstract Analysis getManifest();

    public abstract String getIdentifier();

    public abstract int run() throws Exception;

    /*
     *  Util methods
     */

    final void init(Logger logger, ObjectMap configuration, CatalogManager catalogManager,
                    StorageManagerFactory storageManagerFactory, int studyId, String sessionId) {
        if (initialized) {
            throw new IllegalStateException("The plugin was already initialized! Can't init twice");
        }
        this.logger = logger;
        this.configuration = configuration;
        this.catalogManager = catalogManager;
        this.storageManagerFactory = storageManagerFactory;
        this.studyId = studyId;
        this.sessionId = sessionId;
        initialized = true;
    }

    protected final Logger getLogger() {
        return logger;
    }

    protected final ObjectMap getConfiguration() {
        return configuration;
    }

    protected final CatalogManager getCatalogManager() {
        return catalogManager;
    }

    protected final String getSessionId() {
        return sessionId;
    }

    protected final VariantDBAdaptor getVariantDBAdaptor(int studyId) 
            throws CatalogException, IllegalAccessException, InstantiationException, ClassNotFoundException, 
            StorageManagerException {

        StorageManagerFactory storageManagerFactory = this.storageManagerFactory;
        
        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
        String storageEngine = dataStore.getStorageEngine();
        String dbName = dataStore.getDbName();

        return storageManagerFactory.getVariantStorageManager(storageEngine).getDBAdaptor(dbName);
    }

    protected final AlignmentDBAdaptor getAlignmentDBAdaptor(int studyId) {
        throw new UnsupportedOperationException();
    }

    protected final int getStudyId() {
        return studyId;
    }
}
