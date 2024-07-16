package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.ClientSession;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.NextFlowDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.NextFlowConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.NextFlowCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.nextflow.NextFlow;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.fixAclProjection;

public class NextFlowMongoDBAdaptor extends CatalogMongoDBAdaptor implements NextFlowDBAdaptor {

    private final MongoDBCollection nextflowCollection;
    private final MongoDBCollection archiveNextflowCollection;
    private final MongoDBCollection deleteNextflowCollection;
    private final SnapshotVersionedMongoDBAdaptor versionedMongoDBAdaptor;
    private final NextFlowConverter nextflowConverter;

    public NextFlowMongoDBAdaptor(MongoDBCollection nextflowCollection, MongoDBCollection archiveNextflowCollection,
                                  MongoDBCollection deleteNextflowCollection, Configuration configuration,
                                  OrganizationMongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(NextFlowMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.nextflowCollection = nextflowCollection;
        this.archiveNextflowCollection = archiveNextflowCollection;
        this.deleteNextflowCollection = deleteNextflowCollection;
        this.versionedMongoDBAdaptor = new SnapshotVersionedMongoDBAdaptor(nextflowCollection, archiveNextflowCollection,
                deleteNextflowCollection);
        this.nextflowConverter = new NextFlowConverter();
    }

    @Override
    public OpenCGAResult<NextFlow> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<NextFlow> dbIterator = iterator(studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<NextFlow> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<NextFlow> dbIterator = iterator(query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(studyUid, query, options, user)) {
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
    public DBIterator<NextFlow> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options, user);
        return new NextFlowCatalogMongoDBIterator<>(mongoCursor, null, nextflowConverter, null, studyUid, user, options);
    }

    @Override
    public DBIterator<NextFlow> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options, null);
        return new NextFlowCatalogMongoDBIterator<>(mongoCursor, null, nextflowConverter, null, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, queryOptions, null);
        return new NextFlowCatalogMongoDBIterator(mongoCursor, null, null, null, options);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, queryOptions, user);
        return new NextFlowCatalogMongoDBIterator<>(mongoCursor, null, null, null, studyUid, user, options);
    }

    @Override
    public OpenCGAResult<Long> count(Query query, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        return new OpenCGAResult<>(nextflowCollection.count(null, bson));
    }

    @Override
    public OpenCGAResult<Long> count(Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(nextflowCollection.count(null, bson));
    }

    @Override
    public OpenCGAResult<NextFlow> groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return null;
    }

    @Override
    public OpenCGAResult<NextFlow> groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(nextflowCollection, bsonQuery, field, NextFlowDBAdaptor.QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult<NextFlow> groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(nextflowCollection, bsonQuery, fields, NextFlowDBAdaptor.QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult<?> distinct(long studyUid, String field, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(NextFlowDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);
        return new OpenCGAResult<>(nextflowCollection.distinct(field, bson));
    }

    @Override
    public OpenCGAResult<?> distinct(long studyUid, List<String> fields, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        StopWatch stopWatch = StopWatch.createStarted();
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(NextFlowDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        Set<String> results = new LinkedHashSet<>();
        for (String field : fields) {
            results.addAll(nextflowCollection.distinct(field, bson, String.class).getResults());
        }

        return new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), Collections.emptyList(), results.size(),
                new ArrayList<>(results), -1);
    }

    @Override
    public OpenCGAResult<NextFlow> stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<NextFlow> update(long id, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<NextFlow> update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<NextFlow> delete(NextFlow id) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<NextFlow> delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<NextFlow> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult<NextFlow> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult<NextFlow> rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {

    }


    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = new Query(query);

        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        fixAclProjection(qOptions);

        Bson bson = parseQuery(finalQuery, user);
        MongoDBCollection collection = getQueryCollection(finalQuery, nextflowCollection, archiveNextflowCollection,
                deleteNextflowCollection);
        logger.debug("Nextflow query: {}", bson.toBsonDocument());
        return collection.iterator(clientSession, bson, null, null, qOptions);
    }


    private Bson parseQuery(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null);
    }

    private Bson parseQuery(Query query, String user) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }
}
