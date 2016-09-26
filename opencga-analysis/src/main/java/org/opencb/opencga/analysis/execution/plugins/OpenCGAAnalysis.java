package org.opencb.opencga.analysis.execution.plugins;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.variant.AbstractFileIndexer;
import org.opencb.opencga.catalog.models.tool.Manifest;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created on 26/11/15.
 *
 * TODO: Move non abstract methods to a Context class
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class OpenCGAAnalysis {

    private Logger logger;
    private ObjectMap configuration;
    private CatalogManager catalogManager;
    private String sessionId;
    private boolean initialized;
    private StorageManagerFactory storageManagerFactory;
    private long studyId;

    public Manifest getManifest() {
        try {
            return loadManifest(getIdentifier());
        } catch (IOException ignore) {
            logger.error("Unable to load manifest");
            return null;
        }
    }

    public static Manifest loadManifest(String identifier) throws IOException {
        final String file;
        JsonFactory factory;
        if (OpenCGAAnalysis.class.getResource("/" + identifier + "/manifest.yml") != null) {
            file = "/" + identifier + "/manifest.yml";
            factory = new YAMLFactory();
        } else if (OpenCGAAnalysis.class.getResource("/" + identifier + "/manifest.json") != null) {
            file = "/" + identifier + "/manifest.json";
            factory = new JsonFactory();
        } else if (OpenCGAAnalysis.class.getResource("/" + identifier + "/manifest.xml") != null) {
            file = "/" + identifier + "/manifest.xml";
            factory = new XmlFactory();
        } else {
            return null;
        }
        try (InputStream stream = OpenCGAAnalysis.class.getResourceAsStream(file)) {
            return new ObjectMapper(factory).readValue(stream, Manifest.class);
        }
    }

    public abstract String getIdentifier();

    public abstract int run() throws Exception;

    /*
     *  Util methods
     */

    final void init(Logger logger, ObjectMap configuration, CatalogManager catalogManager,
                    StorageManagerFactory storageManagerFactory, long studyId, String sessionId) {
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

    //TODO: Return a VariantDBAdaptor which checks catalog permissions
    protected final VariantDBAdaptor getVariantDBAdaptor(long studyId)
            throws CatalogException, IllegalAccessException, InstantiationException, ClassNotFoundException, 
            StorageManagerException {

        StorageManagerFactory storageManagerFactory = this.storageManagerFactory;
        
        DataStore dataStore = AbstractFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
        String storageEngine = dataStore.getStorageEngine();
        String dbName = dataStore.getDbName();

        return storageManagerFactory.getVariantStorageManager(storageEngine).getDBAdaptor(dbName);
    }

    //TODO: Return an AlignmentDBAdaptor which checks catalog permissions
    protected final AlignmentDBAdaptor getAlignmentDBAdaptor(long studyId) {
        throw new UnsupportedOperationException();
    }

    protected final long getStudyId() {
        return studyId;
    }
}
