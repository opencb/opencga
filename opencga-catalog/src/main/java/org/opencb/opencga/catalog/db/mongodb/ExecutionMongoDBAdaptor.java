package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ExecutionDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.ExecutionConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.ExecutionCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.job.Execution;
import org.opencb.opencga.core.models.job.JobAclEntry;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

public class ExecutionMongoDBAdaptor extends MongoDBAdaptor implements ExecutionDBAdaptor {

    private final MongoDBCollection executionCollection;
    private final MongoDBCollection deletedExecutionCollection;
    private ExecutionConverter executionConverter;

    private static final String PRIVATE_PRIORITY = "_priority";
    private static final String PRIVATE_STUDY_UIDS = "_studyUids";

    public ExecutionMongoDBAdaptor(MongoDBCollection executionCollection, MongoDBCollection deletedExecutionCollection,
                                   Configuration configuration, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(ExecutionMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.executionCollection = executionCollection;
        this.deletedExecutionCollection = deletedExecutionCollection;
        this.executionConverter = new ExecutionConverter();
    }

    /**
     * @return MongoDB connection to the execution collection.
     */
    public MongoDBCollection getExecutionCollection() {
        return executionCollection;
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> execution, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(execution, "execution");
        return new OpenCGAResult(executionCollection.insert(document, null));
    }

    @Override
    public OpenCGAResult insert(long studyId, Execution execution, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> {
                long tmpStartTime = startQuery();
                logger.debug("Starting execution insert transaction for id '{}'", execution.getId());

                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
                insert(clientSession, studyId, execution);
                return endWrite(tmpStartTime, 1, 1, 0, 0, null);
            });
        } catch (Exception e) {
            logger.error("Could not insert execution {}: {}", execution.getId(), e.getMessage());
            throw e;
        }
    }

    long insert(ClientSession clientSession, long studyId, Execution execution) throws CatalogDBException {
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), execution.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_UID, studyId));

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = executionCollection.count(clientSession, bson);

        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Execution { id: '" + execution.getId() + "'} already exists.");
        }

        long executionUid = getNewUid();
        execution.setUid(executionUid);
        execution.setStudyUid(studyId);
        if (StringUtils.isEmpty(execution.getUuid())) {
            execution.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.EXECUTION));
        }
        if (StringUtils.isEmpty(execution.getCreationDate())) {
            execution.setCreationDate(TimeUtils.getTime());
        }
        if (execution.getPriority() == null) {
            execution.setPriority(Enums.Priority.LOW);
        }

        Document executionObject = executionConverter.convertToStorageType(execution);
        executionObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(execution.getCreationDate()));
        executionObject.put(PRIVATE_MODIFICATION_DATE, executionObject.get(PRIVATE_CREATION_DATE));
//        executionObject.put(PERMISSION_RULES_APPLIED, Collections.emptyList());
//        executionObject.put(PRIVATE_PRIORITY, execution.getPriority().getValue());
        executionObject.put(PRIVATE_STUDY_UIDS, Collections.singletonList(studyId));

        logger.debug("Inserting execution '{}' ({})...", execution.getId(), execution.getUid());
        executionCollection.insert(clientSession, executionObject, null);
        logger.debug("Execution '{}' successfully inserted", execution.getId());
        return executionUid;
    }

    @Override
    public OpenCGAResult<Execution> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Execution> dbIterator = iterator(studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Execution> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Execution> dbIterator = iterator(query, options)) {
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
    public OpenCGAResult nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public DBIterator<Execution> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(query, options, user);
        return new ExecutionCatalogMongoDBIterator(mongoCursor, null, executionConverter, this, dbAdaptorFactory.getCatalogJobDBAdaptor(),
                dbAdaptorFactory.getCatalogFileDBAdaptor(), options, studyUid, user);
    }

    @Override
    public DBIterator<Execution> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(query, options);
        return new ExecutionCatalogMongoDBIterator(mongoCursor, null, executionConverter, this, dbAdaptorFactory.getCatalogJobDBAdaptor(),
                dbAdaptorFactory.getCatalogFileDBAdaptor(), options);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(query, queryOptions, user);
        return new ExecutionCatalogMongoDBIterator(mongoCursor, null, null, this, dbAdaptorFactory.getCatalogJobDBAdaptor(),
                dbAdaptorFactory.getCatalogFileDBAdaptor(), options, studyUid, user);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(query, queryOptions);
        return new ExecutionCatalogMongoDBIterator(mongoCursor, null, null, this, dbAdaptorFactory.getCatalogJobDBAdaptor(),
                dbAdaptorFactory.getCatalogFileDBAdaptor(), options);
    }

    @Override
    public OpenCGAResult<Long> count(Query query, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, QueryOptions.empty(), user);
        logger.debug("Execution count: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return new OpenCGAResult<>(executionCollection.count(bson));
    }

    @Override
    public OpenCGAResult<Execution> groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return null;
    }

    @Override
    public <T> OpenCGAResult<T> distinct(long studyUid, String field, Query query, String userId, Class<T> clazz)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonDocument = parseQuery(query, QueryOptions.empty());
        return new OpenCGAResult<>(executionCollection.count(clientSession, bsonDocument));
    }

    @Override
    public OpenCGAResult<Execution> stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<Execution> update(long id, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Execution> update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Execution> delete(Execution id)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Execution> delete(Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Execution> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult<Execution> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult<Execution> rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Execution> groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Execution> groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Objects.requireNonNull(action);
        try (DBIterator<Execution> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    @Override
    public OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(executionCollection, studyId, permissionRuleId);
    }

    private MongoDBIterator<Document> getMongoCursor(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(query, options, null);
    }

    private MongoDBIterator<Document> getMongoCursor(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = fixOptions(qOptions);

        Bson bson = parseQuery(query, options, user);

        logger.debug("Execution get: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        if (!query.getBoolean(ParamConstants.DELETED_PARAM)) {
            return executionCollection.iterator(bson, qOptions);
        } else {
            return deletedExecutionCollection.iterator(bson, qOptions);
        }
    }

    private QueryOptions fixOptions(QueryOptions queryOptions) {
        QueryOptions options = new QueryOptions(queryOptions);

        filterOptions(options, FILTER_ROUTE_JOBS);
        fixAclProjection(options);
        if (options.containsKey(QueryOptions.SORT)) {
            // If the user is sorting by priority, we will point to the private priority stored as integers to properly sort
            List<String> sortList = options.getAsStringList(QueryOptions.SORT);
            List<String> fixedSortList = new ArrayList<>(sortList.size());
            for (String key : sortList) {
                if (key.startsWith(JobDBAdaptor.QueryParams.PRIORITY.key())) {
                    String[] priorityArray = key.split(":");
                    if (priorityArray.length == 1) {
                        fixedSortList.add(PRIVATE_PRIORITY);
                    } else {
                        // The order (ascending or descending) should be in priorityArray[1]
                        fixedSortList.add(PRIVATE_PRIORITY + ":" + priorityArray[1]);
                    }
                } else {
                    fixedSortList.add(key);
                }
            }
            // Add new fixed sort list
            options.put(QueryOptions.SORT, fixedSortList);
        }

        return options;
    }

    private Bson parseQuery(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, options, null);
    }

    private Bson parseQuery(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, options, user);
    }

    protected Bson parseQuery(Query query, Document extraQuery, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, extraQuery, options, null);
    }

    private Bson parseQuery(Query query, Document extraQuery, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        List<Bson> andBsonList = new ArrayList<>();

        boolean mainStudy = true;
        if (options != null) {
            mainStudy = !options.getBoolean(ParamConstants.OTHER_STUDIES_FLAG, false);
        }

        if (query.containsKey(MongoDBAdaptor.PRIVATE_STUDY_UID)
                && (StringUtils.isNotEmpty(user) || query.containsKey(ParamConstants.ACL_PARAM))) {
            Document studyDocument = getStudyDocument(null, query.getLong(MongoDBAdaptor.PRIVATE_STUDY_UID));

            // Get the document query needed to check the permissions as well
            andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, JobAclEntry.JobPermissions.VIEW.name(),
                    Enums.Resource.EXECUTION, configuration));

            andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.EXECUTION, user,
                    configuration));

            query.remove(ParamConstants.ACL_PARAM);
        }

        Query queryCopy = new Query(query);
        queryCopy.remove(ParamConstants.DELETED_PARAM);

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), queryCopy);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = (QueryParams.getParam(entry.getKey()) != null)
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
                        if (mainStudy) {
                            addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        } else {
                            addAutoOrQuery(PRIVATE_STUDY_UIDS, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        }
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_NAME:
                        // Convert the status to a positive status
                        queryCopy.put(queryParam.key(),
                                Status.getPositiveStatus(Enums.ExecutionStatus.STATUS_LIST, queryCopy.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_NAME.key(), queryParam.key(), queryCopy,
                                QueryParams.INTERNAL_STATUS_NAME.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case USER_ID:
                    case PRIORITY:
                    case RELEASE:
                    case TAGS:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                throw new CatalogDBException(e);
            }
        }

        if (extraQuery != null && extraQuery.size() > 0) {
            andBsonList.add(extraQuery);
        }
        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }
}
