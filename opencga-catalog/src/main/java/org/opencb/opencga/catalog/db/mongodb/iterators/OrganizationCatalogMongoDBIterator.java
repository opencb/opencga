package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

public class OrganizationCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    private final String user;

    private final QueryOptions options;

    private final Queue<Document> organizationListBuffer;

    private final Logger logger;

    private static final int BUFFER_SIZE = 100;

    private static final String UID = OrganizationDBAdaptor.QueryParams.UID.key();

    public OrganizationCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                         GenericDocumentComplexConverter<E> converter, MongoDBAdaptorFactory dbAdaptorFactory,
                                         QueryOptions options, String user) {
        super(mongoCursor, clientSession, converter, null);

        this.options = options != null ? new QueryOptions(options) : new QueryOptions();

        this.user = user;

        this.organizationListBuffer = new LinkedList<>();
        this.logger = LoggerFactory.getLogger(OrganizationCatalogMongoDBIterator.class);
    }

    @Override
    public boolean hasNext() {
        if (organizationListBuffer.isEmpty()) {
            fetchNextBatch();
        }
        return !organizationListBuffer.isEmpty();
    }

    @Override
    public E next() {
        Document next = organizationListBuffer.remove();

        if (filter != null) {
            next = filter.apply(next);
        }

//        addAclInformation(next, options);

        if (converter != null) {
            return (E) converter.convertToDataModelType(next);
        } else {
            return (E) next;
        }
    }

    private void fetchNextBatch() {
        Set<Long> organizationUidSet = new HashSet<>();

        // Get next BUFFER_SIZE documents
        int counter = 0;
        while (mongoCursor.hasNext() && counter < BUFFER_SIZE) {
            Document organizationDocument = mongoCursor.next().get("organizations", Document.class);

            organizationListBuffer.add(organizationDocument);
            counter++;

            if (options == null || !options.containsKey(QueryOptions.EXCLUDE)
                    || !options.getAsStringList(QueryOptions.EXCLUDE).contains("organizations.studies")) {
                organizationUidSet.add(organizationDocument.get(UID, Long.class));
            }
        }

    }
}
