package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.InterpretationConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.MongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.Interpretation;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

public class InterpretationMongoDBAdaptor extends MongoDBAdaptor implements InterpretationDBAdaptor {

    private final MongoDBCollection interpretationCollection;
    private InterpretationConverter interpretationConverter;

    public InterpretationMongoDBAdaptor(MongoDBCollection interpretationCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(InterpretationMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.interpretationCollection = interpretationCollection;
        this.interpretationConverter = new InterpretationConverter();
    }

    public MongoDBCollection getInterpretationCollection() {
        return interpretationCollection;
    }

    @Override
    public void nativeInsert(Map<String, Object> interpretation, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(interpretation, "clinicalAnalysis");
        interpretationCollection.insert(document, null);
    }

    @Override
    public QueryResult<Interpretation> insert(long studyId, Interpretation interpretation, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), interpretation.getInterpretation().getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_ID, studyId));
        filterList.add(Filters.eq(QueryParams.STATUS.key(), Status.READY));

        Bson bson = Filters.and(filterList);
        QueryResult<Long> count = interpretationCollection.count(bson);
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Cannot create interpretation. An interpretation with { id: '"
                    + interpretation.getInterpretation().getId() + "'} already exists.");
        }

        long interpretationUid = getNewId();
        interpretation.setUid(interpretationUid);
        interpretation.setStudyUid(studyId);
        if (StringUtils.isEmpty(interpretation.getUuid())) {
            interpretation.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.INTERPRETATION));
        }

        Document interpretationObject = interpretationConverter.convertToStorageType(interpretation);
        if (StringUtils.isNotEmpty(interpretation.getInterpretation().getCreationDate())) {
            interpretationObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(interpretation.getInterpretation().getCreationDate()));
        } else {
            interpretationObject.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }
        interpretationCollection.insert(interpretationObject, null);

        return endQuery("createInterpretation", startTime, get(interpretationUid, options));
    }

    @Override
    public QueryResult<Interpretation> get(long interpretationId, QueryOptions options) throws CatalogDBException {
        checkId(interpretationId);
        return get(new Query(QueryParams.UID.key(), interpretationId).append(QueryParams.STUDY_UID.key(),
                getStudyId(interpretationId)), options);
    }

    @Override
    public long getStudyId(long interpretationId) throws CatalogDBException {
        Bson query = new Document(PRIVATE_UID, interpretationId);
        Bson projection = Projections.include(PRIVATE_STUDY_ID);
        QueryResult<Document> queryResult = interpretationCollection.find(query, projection, null);

        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(PRIVATE_STUDY_ID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.uidNotFound("Interpretation", interpretationId);
        }
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        logger.debug("Interpretation count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class,
                MongoClient.getDefaultCodecRegistry()));
        return interpretationCollection.count(bson);
    }

    @Override
    public QueryResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermission)
            throws CatalogDBException {
        return count(query);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Interpretation> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Interpretation> documentList = new ArrayList<>();
        try (DBIterator<Interpretation> dbIterator = iterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        QueryResult<Interpretation> queryResult = endQuery("Get", startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult<Interpretation> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return get(query, options);
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        QueryResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Native get", startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options, String user) throws CatalogDBException {
        return nativeGet(query, options);
    }

    @Override
    public QueryResult<Interpretation> update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        Query query = new Query(QueryParams.UID.key(), id);
        UpdateDocument updateDocument = parseAndValidateUpdateParams(parameters, query, queryOptions);

        if (queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            createNewVersion(query);
        }

        Document updateOperation = updateDocument.toFinalUpdateDocument();

        if (!updateOperation.isEmpty()) {
            Bson bsonQuery = Filters.eq(PRIVATE_UID, id);

            logger.debug("Update interpretation. Query: {}, Update: {}", bsonQuery.toBsonDocument(Document.class,
                    MongoClient.getDefaultCodecRegistry()), updateDocument);
            QueryResult<UpdateResult> update = interpretationCollection.update(bsonQuery, updateOperation, null);

            if (update.getResult().isEmpty() || update.getResult().get(0).getMatchedCount() == 0) {
                throw CatalogDBException.uidNotFound("Interpretation", id);
            }
        }

        return endQuery("Modify interpretation", startTime, get(id, queryOptions));
    }

    private UpdateDocument parseAndValidateUpdateParams(ObjectMap parameters, Query query, QueryOptions queryOptions)
            throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        if (parameters.containsKey(UpdateParams.ID.key())) {
            // That can only be done to one individual...
            Query tmpQuery = new Query(query);

            QueryResult<Interpretation> interpretationQueryResult = get(tmpQuery, new QueryOptions());
            if (interpretationQueryResult.getNumResults() == 0) {
                throw new CatalogDBException("Update interpretation: No interpretation found to be updated");
            }
            if (interpretationQueryResult.getNumResults() > 1) {
                throw new CatalogDBException("Update interpretation: Cannot set the same id parameter for different interpretations");
            }

            // Check that the new clinical analysis id will be unique
            long studyId = getStudyId(interpretationQueryResult.first().getUid());

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            QueryResult<Long> count = count(tmpQuery);
            if (count.getResult().get(0) > 0) {
                throw new CatalogDBException("Cannot set id for interpretation. A interpretation with { id: '"
                        + parameters.get(QueryParams.ID.key()) + "'} already exists.");
            }

            document.getSet().put(UpdateParams.ID.key(), parameters.get(UpdateParams.ID.key()));
        }

        String[] acceptedParams = {UpdateParams.DESCRIPTION.key(), UpdateParams.STATUS.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        final String[] acceptedMapParams = {UpdateParams.ATTRIBUTES.key(), UpdateParams.FILTERS.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        String[] objectAcceptedParams = {UpdateParams.PANELS.key(), UpdateParams.SOFTWARE.key(), UpdateParams.ANALYST.key(),
                UpdateParams.DEPENDENCIES.key(), UpdateParams.REPORTED_VARIANTS.key(), UpdateParams.REPORTED_LOW_COVERAGE.key()};
        filterObjectParams(parameters, document.getSet(), objectAcceptedParams);

        Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
        String operation = (String) actionMap.getOrDefault(UpdateParams.COMMENTS.key(), "ADD");
        objectAcceptedParams = new String[]{UpdateParams.COMMENTS.key()};
        switch (operation) {
            case "SET":
                filterObjectParams(parameters, document.getSet(), objectAcceptedParams);
                break;
            case "REMOVE":
                filterObjectParams(parameters, document.getPullAll(), objectAcceptedParams);
                break;
            case "ADD":
            default:
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                break;
        }

        operation = (String) actionMap.getOrDefault(UpdateParams.REPORTED_VARIANTS.key(), "ADD");
        objectAcceptedParams = new String[]{UpdateParams.REPORTED_VARIANTS.key()};
        switch (operation) {
            case "SET":
                filterObjectParams(parameters, document.getSet(), objectAcceptedParams);
                break;
            case "REMOVE":
                filterObjectParams(parameters, document.getPullAll(), objectAcceptedParams);
                break;
            case "ADD":
            default:
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                break;
        }

        if (!document.toFinalUpdateDocument().isEmpty()) {
            // Update modificationDate param
            String time = TimeUtils.getTime();
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }

        return document;
    }

    /**
     * Creates a new version for all the interpretations matching the query.
     *
     * @param query Query object.
     */
    private void createNewVersion(Query query) throws CatalogDBException {
        QueryResult<Document> queryResult = nativeGet(query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

        for (Document document : queryResult.getResult()) {
            Document updateOldVersion = new Document();

            updateOldVersion.put(LAST_OF_VERSION, false);

            // Perform the update on the previous version
            Document queryDocument = new Document()
                    .append(PRIVATE_STUDY_ID, document.getLong(PRIVATE_STUDY_ID))
                    .append(QueryParams.VERSION.key(), document.getInteger(QueryParams.VERSION.key()))
                    .append(PRIVATE_UID, document.getLong(PRIVATE_UID));
            QueryResult<UpdateResult> updateResult = interpretationCollection.update(queryDocument, new Document("$set", updateOldVersion),
                    null);
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogDBException("Internal error: Could not update interpretation");
            }

            // We update the information for the new version of the document
            document.put(LAST_OF_VERSION, true);
            document.put(QueryParams.VERSION.key(), document.getInteger(QueryParams.VERSION.key()) + 1);

            // Insert the new version document
            interpretationCollection.insert(document, QueryOptions.empty());
        }
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public void delete(long id) throws CatalogDBException {

    }

    @Override
    public void delete(Query query) throws CatalogDBException {

    }

    @Override
    public QueryResult<Interpretation> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public DBIterator<Interpretation> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new MongoDBIterator<>(mongoCursor, interpretationConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions);
        return new MongoDBIterator(mongoCursor);
    }

    @Override
    public DBIterator<Interpretation> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException {
        return iterator(query, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException {
        return nativeIterator(query, options);
    }


    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        logger.debug("Interpretation query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return interpretationCollection.nativeQuery().find(bson, qOptions).iterator();
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        try (DBIterator<Interpretation> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    protected Bson parseQuery(Query query, boolean isolated) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        if (isolated) {
            andBsonList.add(new Document("$isolated", 1));
        }

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), query);


        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case UID:
                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
                        addAutoOrQuery(PRIVATE_STUDY_ID, queryParam.key(), query, queryParam.type(), andBsonList);
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
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    // Other parameter that can be queried.
                    case ID:
                    case UUID:
                    case CLINICAL_ANALYSIS:
                    case DESCRIPTION:
                    case STATUS:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                logger.error("Error with " + entry.getKey() + " " + entry.getValue());
                throw new CatalogDBException(e);
            }
        }

        if (!andBsonList.isEmpty()) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }
}
