package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.NotificationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.NotificationConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.notification.Notification;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.filterObjectParams;

public class NotificationMongoDBAdaptor extends CatalogMongoDBAdaptor implements NotificationDBAdaptor {

    private final NotificationConverter notificationConverter;
    private final MongoDBCollection notificationCollection;

    public NotificationMongoDBAdaptor(MongoDBCollection notificationCollection, Configuration configuration,
                                      OrganizationMongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(NotificationMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.notificationCollection = notificationCollection;
        this.notificationConverter = new NotificationConverter();

    }

    @Override
    public OpenCGAResult<Notification> insert(long studyUid, List<Notification> notificationList, QueryOptions options)
            throws CatalogException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting notification insert transaction");
            dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyUid);
            for (Notification notification : notificationList) {
                insert(clientSession, studyUid, notification);
            }

            return endWrite(tmpStartTime, notificationList.size(), notificationList.size(), 0, 0, null);
        }, e -> logger.error("Could not insert notifications: {}", e.getMessage()));
    }

    Notification insert(ClientSession clientSession, long studyUid, Notification notification)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {

        long notificationUid = getNewUid(clientSession);
        notification.setUid(notificationUid);
        notification.setStudyUid(studyUid);

        Document notificationObject = notificationConverter.convertToStorageType(notification);

        String date = notification.getInternal().getRegistrationDate();
        // Versioning private parameters
        notificationObject.put(PRIVATE_CREATION_DATE, StringUtils.isNotEmpty(date) ? TimeUtils.toDate(date) : TimeUtils.getDate());
        notificationObject.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(date) ? TimeUtils.toDate(date) : TimeUtils.getDate());

        logger.debug("Inserting notification '{}' ({})...", notification.getId(), notification.getUid());
        notificationCollection.insert(clientSession, notificationObject, null);
        logger.debug("Notification '{}' successfully inserted", notification.getId());

        return notification;
    }

    @Override
    public OpenCGAResult<Notification> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Notification> dbIterator = iterator(studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Notification> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return get(null, query, options);
    }

    OpenCGAResult<Notification> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Notification> dbIterator = iterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Document> nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeGet(null, studyUid, query, options, user);
    }

    public OpenCGAResult<Document> nativeGet(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    OpenCGAResult<Document> nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public DBIterator<Notification> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options, user);
        return new CatalogMongoDBIterator<>(mongoCursor, null, notificationConverter, null);
    }

    DBIterator<Notification> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new CatalogMongoDBIterator<>(mongoCursor, clientSession, notificationConverter, null);
    }

    @Override
    public DBIterator<Notification> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return iterator(null, query, options);
    }

    @Override
    public DBIterator<Document> nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeIterator(null, query, options);
    }

    DBIterator<Document> nativeIterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new CatalogMongoDBIterator<>(mongoCursor, clientSession, null, null);
    }

    @Override
    public DBIterator<Document> nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeIterator(null, studyUid, query, options, user);
    }

    DBIterator<Document> nativeIterator(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions, user);
        return new CatalogMongoDBIterator<>(mongoCursor, clientSession, null, null);
    }

    @Override
    public OpenCGAResult<Notification> update(long studyUid, String notificationUuid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.UUID.key(), notificationUuid);
        return update(query, parameters, queryOptions);
    }

    @Override
    public OpenCGAResult<Notification> update(long notificationUid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), notificationUid);
        return update(query, parameters, queryOptions);
    }

    @Override
    public OpenCGAResult<Notification> update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        UpdateDocument updateParams = parseAndValidateUpdateParams(parameters);
        Document notificationUpdate = updateParams.toFinalUpdateDocument();

        if (notificationUpdate.isEmpty()) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("NotificationUpdateParams is empty. Nothing to be updated.");
        }

        Bson finalQuery = parseQuery(query);
        logger.debug("Notification update: query : {}, update: {}", finalQuery.toBsonDocument(), notificationUpdate.toBsonDocument());
        DataResult result = notificationCollection.update(finalQuery, notificationUpdate, QueryOptions.empty());

        return endWrite(tmpStartTime, result);
    }

    private UpdateDocument parseAndValidateUpdateParams(ObjectMap parameters) throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        final String[] acceptedObjectParams = {QueryParams.INTERNAL_STATUS.key(), QueryParams.INTERNAL_NOTIFICATIONS.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            document.getSet().put(INTERNAL_LAST_MODIFIED, time);
        }

        return document;
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(query, null);
    }

    @Override
    public OpenCGAResult<Long> count(Query query, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = new Query(query);
        Bson bson = parseQuery(finalQuery, user);
        logger.debug("Notification count query: {}", bson.toBsonDocument());
        return new OpenCGAResult<>(notificationCollection.count(bson));
    }

    @Override
    public OpenCGAResult<Notification> groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        Bson bsonQuery = parseQuery(query, user);
        return groupBy(notificationCollection, bsonQuery, fields, QueryParams.UUID.key(), options);
    }

    @Override
    public OpenCGAResult<?> distinct(long studyUid, String field, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);
        return new OpenCGAResult<>(notificationCollection.distinct(field, bson));
    }

    @Override
    public OpenCGAResult<?> distinct(long studyUid, List<String> fields, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        StopWatch stopWatch = StopWatch.createStarted();
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        Set<String> results = new LinkedHashSet<>();
        for (String field : fields) {
            results.addAll(notificationCollection.distinct(field, bson, String.class).getResults());
        }

        return new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), Collections.emptyList(), results.size(),
                new ArrayList<>(results), -1);
    }

    @Override
    public OpenCGAResult<FacetField> facet(long studyUid, Query query, String facet, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, userId);
        return facet(notificationCollection, bson, facet);
    }

    @Override
    public OpenCGAResult<Notification> stats(Query query) {
        throw new NotImplementedException("Stats not implemented");
    }

    @Override
    public OpenCGAResult<Notification> delete(Notification id)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Delete not implemented");
    }

    @Override
    public OpenCGAResult<Notification> delete(Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Delete not implemented");
    }

    @Override
    public OpenCGAResult<Notification> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Restore not implemented");
    }

    @Override
    public OpenCGAResult<Notification> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Restore not implemented");
    }

    @Override
    public OpenCGAResult<Notification> rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return rank(notificationCollection, bsonQuery, field, QueryParams.UUID.key(), numResults, asc);
    }

    @Override
    public OpenCGAResult<Notification> groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(notificationCollection, bsonQuery, field, QueryParams.UUID.key(), options);
    }

    @Override
    public OpenCGAResult<Notification> groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(notificationCollection, bsonQuery, fields, QueryParams.UUID.key(), options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Objects.requireNonNull(action);
        try (DBIterator<Notification> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException {
        return getMongoCursor(clientSession, query, options, null);
    }


    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException {
        Query finalQuery = new Query(query);

        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        Bson bson = parseQuery(finalQuery, user);
        logger.debug("Notification query: {}", bson.toBsonDocument());
        return notificationCollection.iterator(clientSession, bson, null, null, qOptions);
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        return parseQuery(query, null);
    }

    private Bson parseQuery(Query query, String user) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        Query queryCopy = new Query(query);
        if (StringUtils.isNotEmpty(user)) {
            queryCopy.put(QueryParams.TARGET.key(), user);
        }

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
                    case STUDY_UID:
                        addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case INTERNAL_STATUS_ID:
                    case TARGET:
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

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

}
