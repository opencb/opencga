package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CatalogDBIterator;
import org.opencb.opencga.catalog.db.api.CatalogDatasetDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.DatasetConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Dataset;
import org.opencb.opencga.catalog.models.Status;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.filterOptions;
import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.getMongoDBDocument;

/**
 * Created by pfurio on 04/05/16.
 */
public class CatalogMongoDatasetDBAdaptor extends CatalogMongoDBAdaptor implements CatalogDatasetDBAdaptor {

    private final MongoDBCollection datasetCollection;
    private DatasetConverter datasetConverter;

    /***
     * CatalogMongoDatasetDBAdaptor constructor.
     *
     * @param datasetCollection MongoDB connection to the dataset collection.
     * @param dbAdaptorFactory Generic dbAdaptorFactory containing all the different collections.
     */
    public CatalogMongoDatasetDBAdaptor(MongoDBCollection datasetCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoFileDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.datasetCollection = datasetCollection;
        this.datasetConverter = new DatasetConverter();
    }

    /**
     *
     * @return MongoDB connection to the file collection.
     */
    public MongoDBCollection getDatasetCollection() {
        return datasetCollection;
    }

    @Override
    public QueryResult<Dataset> createDataset(long studyId, Dataset dataset, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);

        Query query = new Query(QueryParams.NAME.key(), dataset.getName()).append(QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Long> count = count(query);

        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Dataset { name: \"" + dataset.getName() + "\" } already exists in the study { " + studyId
                    + " }.");
        }

        long newId = getNewId();
        dataset.setId(newId);

        Document datasetObject = getMongoDBDocument(dataset, "Dataset");
        datasetCollection.insert(datasetObject, null);

        return endQuery("createDataset", startTime, getDataset(dataset.getId(), options));
    }

    @Override
    public long getStudyIdByDatasetId(long datasetId) throws CatalogDBException {
        checkDatasetId(datasetId);
        Query query = new Query(QueryParams.ID.key(), datasetId);
        QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, PRIVATE_STUDY_ID);
        Document dataset = (Document) nativeGet(query, queryOptions).first();

        return dataset.getLong(PRIVATE_STUDY_ID);
    }

    @Override
    public QueryResult<Dataset> getDataset(long datasetId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        Query query = new Query(QueryParams.ID.key(), datasetId);
        return endQuery("Get dataset", startTime, get(query, options));
    }

    @Override
    public QueryResult<Dataset> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        }

        Bson bson;
        try {
            bson = parseQuery(query);
        } catch (NumberFormatException e) {
            throw new CatalogDBException("Get dataset: Could not parse all the arguments from query - " + e.getMessage(), e.getCause());
        }
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_DATASETS);

        QueryResult<Dataset> datasetQueryResult = datasetCollection.find(bson, datasetConverter, qOptions);
        logger.debug("Dataset get: query : {}, project: {}, dbTime: {}", bson, qOptions == null ? "" : qOptions.toJson(),
                datasetQueryResult.getDbTime());
        return endQuery("Get Dataset", startTime, datasetQueryResult.getResult());
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        }

        Bson bson;
        try {
            bson = parseQuery(query);
        } catch (NumberFormatException e) {
            throw new CatalogDBException("Get dataset: Could not parse all the arguments from query - " + e.getMessage(), e.getCause());
        }
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_DATASETS);

        return datasetCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<Dataset> update(long id, ObjectMap parameters) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Dataset> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Dataset> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> restore(Query query) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return datasetCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        return datasetCollection.distinct(field, bsonDocument);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public CatalogDBIterator<Dataset> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = datasetCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator, datasetConverter);
    }

    @Override
    public CatalogDBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = datasetCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(datasetCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(datasetCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(datasetCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        CatalogDBIterator<Dataset> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null
                    ? QueryParams.getParam(entry.getKey())
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
                logger.error("Error with " + entry.getKey() + " " + entry.getValue());
                throw new CatalogDBException(e);
            }
        }

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

}
