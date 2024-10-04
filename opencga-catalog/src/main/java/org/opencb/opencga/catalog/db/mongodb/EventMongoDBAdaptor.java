package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.EventDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.OpenCgaMongoConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.events.IEventHandler;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.event.CatalogEvent;
import org.opencb.opencga.core.models.event.EventSubscriber;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EventMongoDBAdaptor extends MongoDBAdaptor implements EventDBAdaptor {

    private final MongoDBCollection eventCollection;
    private final MongoDBCollection archiveCollection;
    private final OpenCgaMongoConverter<CatalogEvent> eventConverter;

    public EventMongoDBAdaptor(MongoDBCollection eventCollection, MongoDBCollection archiveCollection, Configuration configuration,
                               OrganizationMongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(EventMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;

        this.eventCollection = eventCollection;
        this.archiveCollection = archiveCollection;

        this.eventConverter = new OpenCgaMongoConverter<>(CatalogEvent.class);
    }

    @Override
    public void insert(CatalogEvent event) throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        runTransaction(session -> {
            long uid = getNewUid(session);
            event.setUid(uid);
            Document document = eventConverter.convertToStorageType(event);
            document.put(PRIVATE_UID, uid);
            document.put(PRIVATE_CREATION_DATE,
                    StringUtils.isNotEmpty(event.getCreationDate()) ? TimeUtils.toDate(event.getCreationDate()) : TimeUtils.getDate());
            document.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(event.getModificationDate())
                    ? TimeUtils.toDate(event.getModificationDate()) : TimeUtils.getDate());
            return eventCollection.insert(session, document, QueryOptions.empty());
        });
    }

    @Override
    public void updateSubscriber(CatalogEvent event, Enums.Resource resource, boolean successful)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        runTransaction(session -> {
            Bson query = Filters.and(
                    Filters.eq(PRIVATE_UID, event.getUid()),
                    Filters.elemMatch(QueryParams.SUBSCRIBERS.key(), Filters.eq(SubscribersQueryParams.ID.key(), resource.name()))
            );
            Bson update = Updates.combine(
                    Updates.inc(QueryParams.SUBSCRIBERS.key() + ".$." + SubscribersQueryParams.NUM_ATTEMPTS.key(), 1),
                    Updates.set(QueryParams.SUBSCRIBERS.key() + ".$." + SubscribersQueryParams.SUCCESSFUL.key(), successful)
            );

            logger.debug("Event update: query : {}, update: {}", query.toBsonDocument(), update.toBsonDocument());
            return eventCollection.update(session, query, update, QueryOptions.empty());
        });

    }

    @Override
    public void finishEvent(CatalogEvent opencgaEvent) throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        runTransaction(session -> {
            Query query = new Query(QueryParams.UID.key(), opencgaEvent.getUid());
            Document eventDoc = nativeGet(session, query, QueryOptions.empty()).first();
            CatalogEvent catalogEvent = eventConverter.convertToDataModelType(eventDoc);

            // Check all subscribers performed successfully their action
            boolean archive = true;
            boolean isSuccessful = true;
            for (EventSubscriber subscriber : catalogEvent.getSubscribers()) {
                if (!subscriber.isSuccessful()) {
                    isSuccessful = false;
                    if (subscriber.getNumAttempts() < IEventHandler.MAX_NUM_ATTEMPTS) {
                        archive = false;
                    }
                }
            }

            if (archive) {
                // Move to different collection
                eventDoc.put(QueryParams.SUCCESSFUL.key(), isSuccessful);
                archiveCollection.insert(session, eventDoc, QueryOptions.empty());
                Bson bsonQuery = parseQuery(query);
                eventCollection.remove(session, bsonQuery, QueryOptions.empty());
            }

            return null;
        });
    }

    OpenCGAResult<CatalogEvent> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<CatalogEvent> dbIterator = iterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    DBIterator<CatalogEvent> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new CatalogMongoDBIterator<>(mongoCursor, clientSession, eventConverter, null);
    }

    public OpenCGAResult<Document> nativeGet(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    DBIterator<Document> nativeIterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new CatalogMongoDBIterator<>(mongoCursor, clientSession, null, null);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException {
        Query finalQuery = new Query(query);

        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        Bson bson = parseQuery(finalQuery);
        MongoDBCollection collection = getQueryCollection(finalQuery, eventCollection, archiveCollection, null);
        logger.debug("Event query: {}", bson.toBsonDocument());
        return collection.iterator(clientSession, bson, null, null, qOptions);
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        Query queryCopy = new Query(query);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null
                    ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case UID:
                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case SUCCESSFUL:
                    case ID:
                    case UUID:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                if (e instanceof CatalogDBException) {
                    throw e;
                } else {
                    throw new CatalogDBException("Error parsing query : " + queryCopy.toJson(), e);
                }
            }
        }

        if (!andBsonList.isEmpty()) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }
}
