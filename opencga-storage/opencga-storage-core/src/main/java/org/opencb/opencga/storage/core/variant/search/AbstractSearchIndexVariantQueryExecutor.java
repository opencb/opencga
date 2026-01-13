package org.opencb.opencga.storage.core.variant.search;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
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
    protected final VariantDBAdaptor dbAdaptor;
    private final StorageConfiguration configuration;

    public AbstractSearchIndexVariantQueryExecutor(
            VariantDBAdaptor dbAdaptor, VariantSearchManager searchManager, String storageEngineId,
            StorageConfiguration configuration, ObjectMap options) {
        super(dbAdaptor.getMetadataManager(), storageEngineId, options);
        this.dbAdaptor = dbAdaptor;
        this.searchManager = searchManager;
        this.configuration = configuration;
    }

    protected boolean searchActiveAndAlive(SearchIndexMetadata indexMetadata) {
        return configuration.getSearch().isActive() && searchManager != null && searchManager.isAlive(indexMetadata);
    }

}
