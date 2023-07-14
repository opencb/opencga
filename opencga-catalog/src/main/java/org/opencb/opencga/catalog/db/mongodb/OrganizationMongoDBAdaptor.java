package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.ClientSession;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.OrganizationConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.ProjectCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.organizations.Organization;
import org.slf4j.LoggerFactory;

public class OrganizationMongoDBAdaptor extends MongoDBAdaptor implements OrganizationDBAdaptor {

    private final MongoDBCollection organizationCollection;
    private final MongoDBCollection deletedOrganizationCollection;
    private OrganizationConverter organizationConverter;

    public OrganizationMongoDBAdaptor(MongoDBCollection organizationCollection, MongoDBCollection deletedOrganizationCollection,
                                      Configuration configuration, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(ProjectMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.organizationCollection = organizationCollection;
        this.deletedOrganizationCollection = deletedOrganizationCollection;
        this.organizationConverter = new OrganizationConverter();
    }

    @Override
    public DBIterator<Organization> iterator(Query query, QueryOptions options) throws CatalogDBException {
        return iterator(null, query, options);
    }

    public DBIterator<Organization> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new ProjectCatalogMongoDBIterator<>(mongoCursor, clientSession, organizationConverter, dbAdaptorFactory, options, null);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options) {
        return null;
    }
}
