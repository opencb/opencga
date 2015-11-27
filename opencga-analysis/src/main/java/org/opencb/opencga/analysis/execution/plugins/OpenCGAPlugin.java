package org.opencb.opencga.analysis.execution.plugins;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.beans.Analysis;
import org.opencb.opencga.catalog.CatalogManager;
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

    public abstract Analysis getManifest();

    public abstract String getIdentifier();

    public abstract int run() throws Exception;

    /*
     *  Util methods
     */

    final void init(Logger logger, ObjectMap configuration, CatalogManager catalogManager, String sessionId) {
        if (initialized) {
            throw new IllegalStateException("The plugin was already initialized! Can't init twice");
        }
        this.logger = logger;
        this.configuration = configuration;
        this.catalogManager = catalogManager;
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

    protected final VariantDBAdaptor getVariantDBAdaptor(int studyId) {
        throw new UnsupportedOperationException();
    }

    protected final AlignmentDBAdaptor getAlignmentDBAdaptor(int studyId) {
        throw new UnsupportedOperationException();
    }
}
