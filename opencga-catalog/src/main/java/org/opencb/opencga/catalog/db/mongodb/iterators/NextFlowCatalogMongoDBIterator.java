package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

public class NextFlowCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    private long studyUid;
    private String user;

    private QueryOptions options;
    private Queue<Document> nextflowListBuffer;

    private Logger logger;

    private static final int BUFFER_SIZE = 100;
    private static final String UID_VERSION_SEP = "___";

    public NextFlowCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                                GenericDocumentComplexConverter<E> converter,
                                                OrganizationMongoDBAdaptorFactory dbAdaptorFactory, QueryOptions options) {
        this(mongoCursor, clientSession, converter, dbAdaptorFactory, 0, null, options);
    }

    public NextFlowCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                          GenericDocumentComplexConverter<E> converter, OrganizationMongoDBAdaptorFactory dbAdaptorFactory,
                                          long studyUid, String user, QueryOptions options) {
        super(mongoCursor, clientSession, converter, null);

        this.user = user;
        this.studyUid = studyUid;

        this.options = options;
        this.nextflowListBuffer = new LinkedList<>();
        this.logger = LoggerFactory.getLogger(NextFlowCatalogMongoDBIterator.class);
    }

}
