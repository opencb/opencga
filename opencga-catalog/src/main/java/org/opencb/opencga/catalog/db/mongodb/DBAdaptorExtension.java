package org.opencb.opencga.catalog.db.mongodb;

import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.catalog.db.api.DBAdaptor;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.PrivateFields;

import java.util.Map;

/**
 * Interface for extending database adaptor functionality with custom collections and indexes.
 */
public interface DBAdaptorExtension {

    /**
     * Creates the custom collections required by this extension in the MongoDB database.
     *
     * @param mongoDataStore the MongoDB data store where collections will be created
     */
    void createCollections(MongoDataStore mongoDataStore);

    /**
     * Creates the necessary indexes for the custom collections managed by this extension.
     *
     * @param mongoDataStore the MongoDB data store where indexes will be created
     */
    void createIndexes(MongoDataStore mongoDataStore);

    /**
     * Provides a mapping of PrivateFields classes to their corresponding CatalogMongoDBAdaptor instances.
     *
     * @param mongoDataStore the MongoDB data store used to create adaptors
     * @param dbAdaptorFactory the database adaptor factory for the organization
     * @param configuration  the configuration object for the catalog
     * @return a map linking PrivateFields classes to their CatalogMongoDBAdaptor instances
     */
    Map<Class<? extends PrivateFields>, DBAdaptor<?>> getAdaptors(MongoDataStore mongoDataStore,
                                                                  OrganizationMongoDBAdaptorFactory dbAdaptorFactory,
                                                                  Configuration configuration);

}
