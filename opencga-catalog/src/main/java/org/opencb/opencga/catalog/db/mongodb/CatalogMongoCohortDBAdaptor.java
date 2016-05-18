package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogDBIterator;
import org.opencb.opencga.catalog.db.mongodb.converters.CohortConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

public class CatalogMongoCohortDBAdaptor extends CatalogMongoDBAdaptor implements CatalogCohortDBAdaptor {

    private final MongoDBCollection cohortCollection;
    private CohortConverter cohortConverter;

    public CatalogMongoCohortDBAdaptor(MongoDBCollection cohortCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoSampleDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.cohortCollection = cohortCollection;
        this.cohortConverter = new CohortConverter();
    }

    @Override
    public QueryResult<Cohort> createCohort(long studyId, Cohort cohort, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        checkCohortNameExists(studyId, cohort.getName());

        long newId = dbAdaptorFactory.getCatalogMetaDBAdaptor().getNewAutoIncrementId();
        cohort.setId(newId);

        Document cohortObject = cohortConverter.convertToStorageType(cohort);
        cohortObject.append(PRIVATE_STUDY_ID, studyId);
        cohortObject.append(PRIVATE_ID, newId);

        try {
            cohortCollection.insert(cohortObject, null);
        } catch (DuplicateKeyException e) {
            throw CatalogDBException.alreadyExists("Cohort from study { id:" + studyId + "}", newId);
        } catch (MongoWriteException e) {
            throw CatalogDBException.alreadyExists("Cohort from study { id:" + studyId + "}", newId);
        }

        return endQuery("createCohort", startTime, getCohort(newId, options));
    }

    @Override
    public QueryResult<Cohort> getCohort(long cohortId, QueryOptions options) throws CatalogDBException {
        return get(new Query(QueryParams.ID.key(), cohortId), options);
    }

    @Override
    public QueryResult<Cohort> getAllCohorts(long studyId, QueryOptions options) throws CatalogDBException {
        return get(new Query(QueryParams.STUDY_ID.key(), studyId), options);
    }

    @Override
    public QueryResult<Cohort> modifyCohort(long cohortId, ObjectMap parameters, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        update(new Query(QueryParams.ID.key(), cohortId), parameters);
        return endQuery("Modify cohort", startTime, getCohort(cohortId, options));
    }

    @Override
    public QueryResult<Cohort> deleteCohort(long cohortId, QueryOptions queryOptions) throws CatalogDBException {
        return delete(cohortId, queryOptions);
    }

    @Override
    public long getStudyIdByCohortId(long cohortId) throws CatalogDBException {
        QueryResult queryResult = nativeGet(new Query(QueryParams.ID.key(), cohortId),
                new QueryOptions(MongoDBCollection.INCLUDE, PRIVATE_STUDY_ID));
        if (queryResult.getResult().isEmpty()) {
            throw CatalogDBException.idNotFound("Cohort", cohortId);
        } else {
            return ((Document) queryResult.first()).getLong(PRIVATE_STUDY_ID);
        }
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        long startTime = startQuery();
        return endQuery("Count cohort", startTime, cohortCollection.count(parseQuery(query)));
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return cohortCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Cohort> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_COHORTS);
        QueryResult<Cohort> cohortQueryResult = cohortCollection.find(bson, cohortConverter, qOptions);
        return endQuery("Get cohort", startTime, cohortQueryResult);
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_COHORTS);
        return cohortCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<Cohort> update(long id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        update(new Query(QueryParams.ID.key(), id), parameters);
        return endQuery("Update cohort", startTime, getCohort(id, new QueryOptions()));
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> cohortParams = new HashMap<>();

        String[] acceptedParams = {QueryParams.DESCRIPTION.key(), QueryParams.NAME.key(), QueryParams.CREATION_DATE.key()};
        filterStringParams(parameters, cohortParams, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap(QueryParams.TYPE.key(), Cohort.Type.class);
        filterEnumParams(parameters, cohortParams, acceptedEnums);

        String[] acceptedLongListParams = {QueryParams.SAMPLES.key()};
        filterLongParams(parameters, cohortParams, acceptedLongListParams);
        if (parameters.containsKey(QueryParams.SAMPLES.key())) {
            for (Long sampleId : parameters.getAsLongList(QueryParams.SAMPLES.key())) {
                if (!dbAdaptorFactory.getCatalogSampleDBAdaptor().sampleExists(sampleId)) {
                    throw CatalogDBException.idNotFound("Sample", sampleId);
                }
            }
        }

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.STATS.key()};
        filterMapParams(parameters, cohortParams, acceptedMapParams);

        //Map<String, Class<? extends Enum>> acceptedEnumParams = Collections.singletonMap(QueryParams.STATUS_STATUS.key(),
        //        Cohort.CohortStatus.class);
        //filterEnumParams(parameters, cohortParams, acceptedEnumParams);
        if (parameters.containsKey(QueryParams.STATUS_STATUS.key())) {
            cohortParams.put(QueryParams.STATUS_STATUS.key(), parameters.get(QueryParams.STATUS_STATUS.key()));
            cohortParams.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTimeMillis());
        }
        if (parameters.containsKey("status")) {
            throw new CatalogDBException("Unable to modify cohort. Use parameter \"" + QueryParams.STATUS_STATUS.key()
                    + "\" instead of \"status\"");
        }

        if (!cohortParams.isEmpty()) {
            QueryResult<UpdateResult> update = cohortCollection.update(parseQuery(query), new Document("$set", cohortParams), null);
            return endQuery("Update cohort", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        return endQuery("Update cohort", startTime, new QueryResult<>());
    }

    @Override
    public QueryResult<Cohort> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkCohortId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        if (count(query).first() == 0) {
            query.put(QueryParams.STATUS_STATUS.key(), Status.DELETED + "," + Status.REMOVED);
            QueryOptions options = new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.STATUS_STATUS.key());
            Cohort cohort = get(query, options).first();
            throw new CatalogDBException("The cohort {" + id + "} was already " + cohort.getStatus().getStatus());
        }

        // Change the status of the cohort to deleted
        setStatus(id, Status.DELETED);

        query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_STATUS.key(), Status.DELETED);

        return endQuery("Delete cohort", startTime, get(query, null));
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        QueryResult<Cohort> cohortQueryResult = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key()));
        for (Cohort cohort : cohortQueryResult.getResult()) {
            delete(cohort.getId(), queryOptions);
        }
        return endQuery("Delete cohort", startTime, Collections.singletonList(cohortQueryResult.getNumTotalResults()));
    }

    QueryResult<Cohort> setStatus(long cohortId, String status) throws CatalogDBException {
        return update(cohortId, new ObjectMap(QueryParams.STATUS_STATUS.key(), status));
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_STATUS.key(), status));
    }

    @Override
    public QueryResult<Cohort> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Operation not yet supported.");
//        long startTime = startQuery();
//        if (queryOptions == null) {
//            queryOptions = new QueryOptions();
//        }
//        QueryResult<Long> remove = remove(new Query(QueryParams.ID.key(), id), queryOptions);
//        if (remove.getResult().get(0) != 1L) {
//            throw CatalogDBException.removeError("Cohort");
//        }
//        Query query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_STATUS.key(), Cohort.CohortStatus.REMOVED);
//        return endQuery("Remove cohort", startTime, get(query, new QueryOptions()));
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Operation not yet supported.");
//        long startTime = startQuery();
//        query.append(QueryParams.STATUS_STATUS.key(), Cohort.CohortStatus.NONE + "," + Cohort.CohortStatus.DELETED);
//
//        // First we obtain the ids of the cohorts that will be removed.
//        List<Cohort> cohorts = get(query, new QueryOptions(MongoDBCollection.INCLUDE,
//                Arrays.asList(QueryParams.ID.key(), QueryParams.STATUS_STATUS))).getResult();
//
//        QueryResult<Long> removed = update(query, new ObjectMap(QueryParams.STATUS_STATUS.key(), Cohort.CohortStatus.REMOVED));
//
//        if (removed.first() != cohorts.size()) {
//            throw CatalogDBException.removeError("Cohort");
//        }
//
//        // Remove the instances to cohort that are stored in study
//        dbAdaptorFactory.getCatalogStudyDBAdaptor().removeCohortDependencies(
//                cohorts.stream()
//                        .filter(c -> c.getStatus().getStatus() != Cohort.CohortStatus.DELETED)
//                        .map(Cohort::getId).collect(Collectors.toList())
//        );
//
//        return endQuery("Remove cohorts", startTime, Collections.singletonList(removed.first()));
    }

    @Override
    public QueryResult<Long> restore(Query query) throws CatalogDBException {
        throw new UnsupportedOperationException("Operation not yet supported.");
//        long startTime = startQuery();
//        query.append(QueryParams.STATUS_STATUS.key(), Cohort.CohortStatus.DELETED);
//        QueryResult<Long> updateStatus = update(query, new ObjectMap(QueryParams.STATUS_STATUS.key(), Cohort.CohortStatus.NONE));
////        QueryResult<Long> updateStatus = updateStatus(query, Cohort.CohortStatus.NONE);
//
//        return endQuery("Restore cohorts", startTime, Collections.singletonList(updateStatus.first()));
    }

//    @Override
//    public QueryResult<Long> updateStatus(Query query, String status) throws CatalogDBException {
//        long startTime = startQuery();
//        QueryResult<UpdateResult> update = cohortCollection.update(parseQuery(query),
//                Updates.combine(
//                        Updates.set(QueryParams.STATUS_STATUS.key(), status),
//                        Updates.set(QueryParams.STATUS_DATE.key(), TimeUtils.getTimeMillis()))
//                , new QueryOptions());
//        return endQuery("Update cohort status", startTime, Collections.singletonList(update.first().getModifiedCount()));
//    }

    @Override
    public CatalogDBIterator<Cohort> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = cohortCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator, cohortConverter);
    }

    @Override
    public CatalogDBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = cohortCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(cohortCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(cohortCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(cohortCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        CatalogDBIterator<Cohort> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private void checkCohortNameExists(long studyId, String cohortName) throws CatalogDBException {
        QueryResult<Long> count = cohortCollection.count(Filters.and(
                Filters.eq(PRIVATE_STUDY_ID, studyId), Filters.eq(QueryParams.NAME.key(), cohortName)));
        if (count.getResult().get(0) > 0) {
            throw CatalogDBException.alreadyExists("Cohort", "name", cohortName);
        }
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        }

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            try {
                switch (queryParam) {
                    case ID:
                        addOrQuery(PRIVATE_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_ID:
                        addOrQuery(PRIVATE_STUDY_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                }
            } catch (Exception e) {
                throw new CatalogDBException(e);
            }
        }

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

    public MongoDBCollection getCohortCollection() {
        return cohortCollection;
    }
}
