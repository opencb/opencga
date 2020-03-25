package org.opencb.opencga.storage.core.variant.search;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.query.executors.VariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;

/**
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractSearchIndexVariantQueryExecutor extends VariantQueryExecutor {
    protected final VariantSearchManager searchManager;
    protected final String dbName;
    protected final VariantDBAdaptor dbAdaptor;
    private final StorageConfiguration configuration;

    public AbstractSearchIndexVariantQueryExecutor(
            VariantDBAdaptor dbAdaptor, VariantSearchManager searchManager, String storageEngineId, String dbName,
            StorageConfiguration configuration, ObjectMap options) {
        super(dbAdaptor.getMetadataManager(), storageEngineId, options);
        this.dbAdaptor = dbAdaptor;
        this.searchManager = searchManager;
        this.dbName = dbName;
        this.configuration = configuration;
    }

    protected boolean searchActiveAndAlive() {
        return searchActiveAndAlive(dbName);
    }

    protected boolean searchActiveAndAlive(String collection) {
        return configuration.getSearch().isActive() && searchManager != null && searchManager.isAlive(collection);
    }
}
