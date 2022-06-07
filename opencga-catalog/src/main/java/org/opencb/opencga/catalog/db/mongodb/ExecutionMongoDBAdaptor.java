package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ExecutionDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.ExecutionConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.ExecutionCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.job.Execution;
import org.opencb.opencga.core.models.job.ExecutionAclEntry;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobInternalWebhook;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
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
    public OpenCGAResult<Execution> insert(long studyId, Execution execution, QueryOptions options)
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

    public long insert(ClientSession clientSession, long studyUid, Execution execution) throws CatalogDBException {
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), execution.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_UID, studyUid));

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = executionCollection.count(clientSession, bson);

        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Execution { id: '" + execution.getId() + "'} already exists.");
        }

        if (CollectionUtils.isNotEmpty(execution.getJobs())) {
            for (Job job : execution.getJobs()) {
                // Create jobs
                long jobUid = dbAdaptorFactory.getCatalogJobDBAdaptor().insert(clientSession, studyUid, job);
                job.setUid(jobUid);
            }
        }

        long executionUid = getNewUid();
        execution.setUid(executionUid);
        execution.setStudyUid(studyUid);
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
        executionObject.put(PRIVATE_PRIORITY, execution.getPriority().getValue());
        executionObject.put(PRIVATE_STUDY_UIDS, Collections.singletonList(studyUid));

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
    public OpenCGAResult<Execution> update(long uid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key()));
        OpenCGAResult<Execution> dataResult = get(uid, options);

        if (dataResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update execution. Execution uid '" + uid + "' not found.");
        }

        try {
            return runTransaction(session -> privateUpdate(session, dataResult.first(), parameters, queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update execution {}: {}", dataResult.first().getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not update execution " + dataResult.first().getId() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult<Execution> update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (parameters.containsKey(QueryParams.ID.key())) {
            // We need to check that the update is only performed over 1 single execution
            if (count(query).getNumMatches() != 1) {
                throw new CatalogDBException("Operation not supported: '" + QueryParams.ID.key() + "' can only be updated for one"
                        + " execution");
            }
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Execution> iterator = iterator(query, options);

        OpenCGAResult<Execution> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Execution execution = iterator.next();
            try {
                result.append(runTransaction(session -> privateUpdate(session, execution, parameters, queryOptions)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not update execution {}: {}", execution.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, execution.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }
        return result;
    }

    OpenCGAResult<Execution> privateUpdate(ClientSession clientSession, Execution execution, ObjectMap parameters, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        Document executionParameters = parseAndValidateUpdateParams(parameters, options).toFinalUpdateDocument();
        if (executionParameters.isEmpty()) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("Nothing to update. Empty 'parameters' object");
        }

        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), execution.getStudyUid())
                .append(QueryParams.UID.key(), execution.getUid());
        Bson finalQuery = parseQuery(tmpQuery, options);

        logger.debug("Execution update: query : {}, update: {}",
                finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                executionParameters.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        DataResult result = executionCollection.update(clientSession, finalQuery, executionParameters, null);

        if (result.getNumMatches() == 0) {
            throw new CatalogDBException("Execution " + execution.getId() + " not found");
        }
        List<Event> events = new ArrayList<>();
        if (result.getNumUpdated() == 0) {
            events.add(new Event(Event.Type.WARNING, execution.getId(), "Nothing updated. Execution already had those values"));
        }
        logger.debug("Execution {} successfully updated", execution.getId());

        return endWrite(tmpStartTime, 1, 1, events);
    }

    private UpdateDocument parseAndValidateUpdateParams(ObjectMap parameters, QueryOptions options) throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        String[] acceptedParams = {QueryParams.USER_ID.key(), QueryParams.DESCRIPTION.key(), QueryParams.TOOL_ID.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        String[] acceptedBooleanParams = {QueryParams.IS_PIPELINE.key(), QueryParams.VISITED.key()};
        filterBooleanParams(parameters, document.getSet(), acceptedBooleanParams);

        String[] acceptedStringListParams = {QueryParams.TAGS.key()};
        filterStringListParams(parameters, document.getSet(), acceptedStringListParams);

        if (parameters.containsKey(QueryParams.INTERNAL_WEBHOOK.key())) {
            Object value = parameters.get(QueryParams.INTERNAL_WEBHOOK.key());
            if (value instanceof JobInternalWebhook) {
                document.getSet().put(QueryParams.INTERNAL_WEBHOOK.key(), getMongoDBDocument(value, "JobInternalWebhook"));
            } else {
                document.getSet().put(QueryParams.INTERNAL_WEBHOOK.key(), value);
            }
        }

        if (parameters.containsKey(QueryParams.INTERNAL_EVENTS.key())) {
            Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());
            ParamUtils.BasicUpdateAction operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.INTERNAL_EVENTS.key(),
                    ParamUtils.BasicUpdateAction.ADD);
            String[] acceptedObjectParams = new String[]{QueryParams.INTERNAL_EVENTS.key()};
            switch (operation) {
                case SET:
                    filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
                    break;
                case REMOVE:
                    filterObjectParams(parameters, document.getPullAll(), acceptedObjectParams);
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), acceptedObjectParams);
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }

//        if (parameters.containsKey(QueryParams.INPUT.key())) {
//            List<Object> fileList = parameters.getList(QueryParams.INPUT.key());
//            document.getSet().put(QueryParams.INPUT.key(), jobConverter.convertFilesToDocument(fileList));
//        }
//        if (parameters.containsKey(QueryParams.OUTPUT.key())) {
//            List<Object> fileList = parameters.getList(QueryParams.OUTPUT.key());
//            document.getSet().put(QueryParams.OUTPUT.key(), jobConverter.convertFilesToDocument(fileList));
//        }
        if (parameters.containsKey(QueryParams.OUT_DIR.key())) {
            document.getSet().put(QueryParams.OUT_DIR.key(),
                    executionConverter.convertFileToDocument(parameters.get(QueryParams.OUT_DIR.key())));
        }
        if (parameters.containsKey(QueryParams.STDOUT.key())) {
            document.getSet().put(QueryParams.STDOUT.key(),
                    executionConverter.convertFileToDocument(parameters.get(QueryParams.STDOUT.key())));
        }
        if (parameters.containsKey(QueryParams.STDERR.key())) {
            document.getSet().put(QueryParams.STDERR.key(),
                    executionConverter.convertFileToDocument(parameters.get(QueryParams.STDERR.key())));
        }

        if (parameters.containsKey(QueryParams.PRIORITY.key())) {
            document.getSet().put(QueryParams.PRIORITY.key(), parameters.getString(QueryParams.PRIORITY.key()));
            document.getSet().put(PRIVATE_PRIORITY,
                    Enums.Priority.getPriority(parameters.getString(QueryParams.PRIORITY.key())).getValue());
        }

        String[] acceptedObjectParams = {QueryParams.PIPELINE.key(), QueryParams.STUDY.key(), QueryParams.INTERNAL_STATUS.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        if (document.getSet().containsKey(QueryParams.STUDY.key())) {
            List<String> studyFqns = new LinkedList<>();
            studyFqns.add(parameters.getString(QueryParams.STUDY_ID.key()));
            studyFqns.addAll(parameters.getAsStringList(QueryParams.STUDY_OTHERS.key()));
            Query query = new Query(StudyDBAdaptor.QueryParams.FQN.key(), studyFqns);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.UID.key());
            OpenCGAResult<Study> studyResults = dbAdaptorFactory.getCatalogStudyDBAdaptor().get(query, queryOptions);
            if (studyResults.getNumResults() < studyFqns.size()) {
                throw new CatalogDBException("Unable to find some studies from '" + studyFqns + "'");
            }

            // Add uids to others array
            document.getSet().put(PRIVATE_STUDY_UIDS,
                    studyResults.getResults().stream().map(Study::getUid).collect(Collectors.toList()));
        }

        String[] acceptedMapParams = {QueryParams.PARAMS.key(), QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
                document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
            }
            document.getSet().put(INTERNAL_LAST_MODIFIED, time);
        }

        return document;
    }

    void updateJobList(ClientSession clientSession, long studyUid, String executionId, List<Job> jobList)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        List<Document> documentList = executionConverter.convertExecutionsOrJobsToDocument(jobList);
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.ID.key(), executionId);
        Bson bsonQuery = parseQuery(query, QueryOptions.empty());

        Bson update = Updates.addEachToSet(QueryParams.JOBS.key(), documentList);
        DataResult result = executionCollection.update(clientSession, bsonQuery, update, QueryOptions.empty());

        if (result.getNumMatches() == 0) {
            throw new CatalogDBException("Could not update list of jobs. Execution '" + executionId + "' not found.");
        }
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
                if (key.startsWith(QueryParams.PRIORITY.key())) {
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
            andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, ExecutionAclEntry.ExecutionPermissions.VIEW.name(),
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
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_ID:
                        // Convert the status to a positive status
                        queryCopy.put(queryParam.key(),
                                InternalStatus.getPositiveStatus(Enums.ExecutionStatus.STATUS_LIST, queryCopy.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_ID.key(), queryParam.key(), queryCopy,
                                QueryParams.INTERNAL_STATUS_ID.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case USER_ID:
                    case PRIORITY:
                    case RELEASE:
                    case TAGS:
                    case VISITED:
                    case IS_PIPELINE:
                    case INTERNAL_TOOL_ID:
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
