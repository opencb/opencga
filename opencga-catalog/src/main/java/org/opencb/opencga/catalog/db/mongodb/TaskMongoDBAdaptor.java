package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.TaskDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.TaskConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.MongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.Task;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.results.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

public class TaskMongoDBAdaptor extends MongoDBAdaptor implements TaskDBAdaptor {

    private MongoDBCollection taskCollection;
    private TaskConverter taskConverter;

    private static final String PRIVATE_PRIORITY = "_priority";

    public TaskMongoDBAdaptor(MongoDBCollection taskCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(TaskMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.taskCollection = taskCollection;
        this.taskConverter = new TaskConverter();
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> task) throws CatalogDBException {
        Document document = getMongoDBDocument(task, "task");
        return new OpenCGAResult(taskCollection.insert(document, null));
    }

    @Override
    public OpenCGAResult insert(long studyUid, Task task, QueryOptions options) throws CatalogDBException {
        try {
            return runTransaction(clientSession -> {
                long tmpStartTime = startQuery();
                logger.debug("Starting task insert transaction for task id '{}'", task.getId());

                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyUid);
                insert(clientSession, studyUid, task);
                return endWrite(tmpStartTime, 1, 1, 0, 0, null);
            });
        } catch (Exception e) {
            logger.error("Could not create task {}: {}", task.getId(), e.getMessage());
            throw e;
        }
    }

    long insert(ClientSession clientSession, long studyId, Task task) throws CatalogDBException {
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), task.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_UID, studyId));

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = taskCollection.count(clientSession, bson);

        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Task { id: '" + task.getId() + "'} already exists.");
        }

        long jobUid = getNewUid(clientSession);
        task.setUid(jobUid);
        task.setStudyUid(studyId);
        if (StringUtils.isEmpty(task.getUuid())) {
            task.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.TASK));
        }
        if (StringUtils.isEmpty(task.getCreationDate())) {
            task.setCreationDate(TimeUtils.getTime());
        }

        Document jobObject = taskConverter.convertToStorageType(task);
        jobObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(task.getCreationDate()));
        jobObject.put(PRIVATE_MODIFICATION_DATE, TimeUtils.toDate(task.getCreationDate()));
        jobObject.put(PRIVATE_PRIORITY, task.getPriority().getValue());

        logger.debug("Inserting task '{}' ({})...", task.getId(), task.getUid());
        taskCollection.insert(clientSession, jobObject, null);
        logger.debug("Task '{}' successfully inserted", task.getId());
        return jobUid;
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        return new OpenCGAResult<>(taskCollection.count(bsonDocument));
    }


    @Override
    public OpenCGAResult<Long> count(long studyUid, Query query, String user, StudyAclEntry.StudyPermissions studyPermission)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult distinct(Query query, String field) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        return new OpenCGAResult(taskCollection.distinct(field, bsonDocument));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<Task> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Task> documentList = new ArrayList<>();
        OpenCGAResult<Task> queryResult;
        try (DBIterator<Task> dbIterator = iterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            OpenCGAResult<Long> count = count(query);
            queryResult.setNumMatches(count.getNumMatches());
        }
        return queryResult;
    }

    @Override
    public OpenCGAResult<Task> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        OpenCGAResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            OpenCGAResult<Long> count = count(query);
            queryResult.setNumMatches(count.getNumMatches());
        }
        return queryResult;
    }

    @Override
    public OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult update(long taskUid, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key()));
        OpenCGAResult<Task> dataResult = get(taskUid, options);

        if (dataResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update task. Task uid '" + taskUid + "' not found.");
        }

        try {
            return runTransaction(session -> privateUpdate(session, dataResult.first(), parameters));
        } catch (CatalogDBException e) {
            logger.error("Could not update task {}: {}", dataResult.first().getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not update task " + dataResult.first().getId() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        if (parameters.containsKey(QueryParams.ID.key())) {
            // We need to check that the update is only performed over 1 single job
            if (count(query).first() != 1) {
                throw new CatalogDBException("Operation not supported: '" + QueryParams.ID.key() + "' can only be updated for one task");
            }
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Task> iterator = iterator(query, options);

        OpenCGAResult<Task> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Task task = iterator.next();
            try {
                result.append(runTransaction(session -> privateUpdate(session, task, parameters)));
            } catch (CatalogDBException e) {
                logger.error("Could not update task {}: {}", task.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, task.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }
        return result;
    }

    OpenCGAResult<Object> privateUpdate(ClientSession clientSession, Task task, ObjectMap parameters) throws CatalogDBException {
        long tmpStartTime = startQuery();

        Document taskParameters = getValidatedUpdateParams(parameters);
        if (taskParameters.isEmpty()) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("Nothing to update. Empty 'parameters' object");
        }

        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), task.getStudyUid())
                .append(QueryParams.UID.key(), task.getUid());
        Bson finalQuery = parseQuery(tmpQuery);

        logger.debug("Task update: query : {}, update: {}",
                finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                taskParameters.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        DataResult result = taskCollection.update(clientSession, finalQuery, new Document("$set", taskParameters), null);

        if (result.getNumMatches() == 0) {
            throw new CatalogDBException("Task " + task.getId() + " not found");
        }
        List<Event> events = new ArrayList<>();
        if (result.getNumUpdated() == 0) {
            events.add(new Event(Event.Type.WARNING, task.getId(), "Task was already updated"));
        }
        logger.debug("Task {} successfully updated", task.getId());

        return endWrite(tmpStartTime, 1, 1, events);
    }

    private Document getValidatedUpdateParams(ObjectMap parameters) throws CatalogDBException {
        Document taskParameters = new Document();

        String[] acceptedParams = {QueryParams.COMMAND_LINE.key(), QueryParams.CREATION_DATE.key(), QueryParams.MODIFICATION_DATE.key(),
                QueryParams.ACTION.key(), QueryParams.RESOURCE.key(),
        };
        filterStringParams(parameters, taskParameters, acceptedParams);

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            taskParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            taskParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (parameters.containsKey(QueryParams.STATUS.key()) && parameters.get(QueryParams.STATUS.key()) instanceof Enums.ExecutionStatus) {
            taskParameters.put(QueryParams.STATUS.key(), getMongoDBDocument(parameters.get(QueryParams.STATUS.key()), "Task.TaskStatus"));
        }

        if (parameters.containsKey(QueryParams.PRIORITY.key())) {
            taskParameters.put(QueryParams.PRIORITY.key(), parameters.getString(QueryParams.PRIORITY.key()));
            taskParameters.put(PRIVATE_PRIORITY, Enums.Priority.getPriority(parameters.getString(QueryParams.PRIORITY.key())).getValue());
        }

        String[] acceptedMapParams = {QueryParams.PARAMS.key(), QueryParams.ATTRIBUTES.key(),
                QueryParams.RESOURCE_MANAGER_ATTRIBUTES.key()};
        filterMapParams(parameters, taskParameters, acceptedMapParams);

        if (!taskParameters.isEmpty()) {
            // Update modificationDate param
            String time = TimeUtils.getTime();
            Date date = TimeUtils.toDate(time);
            taskParameters.put(QueryParams.MODIFICATION_DATE.key(), time);
            taskParameters.put(PRIVATE_MODIFICATION_DATE, date);
        }

        return taskParameters;
    }

    @Override
    public OpenCGAResult delete(Task id) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public DBIterator<Task> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new MongoDBIterator<>(mongoCursor, taskConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions);
        return new MongoDBIterator<>(mongoCursor);
    }

    @Override
    public DBIterator<Task> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> documentMongoCursor;
        try {
            documentMongoCursor = getMongoCursor(query, options, null, null);
        } catch (CatalogAuthorizationException e) {
            throw new CatalogDBException(e);
        }
        return documentMongoCursor;
    }

    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options, Document studyDocument, String user)
            throws CatalogDBException, CatalogAuthorizationException {
//        Document queryForAuthorisedEntries = null;
//        if (studyDocument != null && user != null) {
//            // Get the document query needed to check the permissions as well
//            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
//                    StudyAclEntry.StudyPermissions.VIEW_JOBS.name(), JobAclEntry.JobPermissions.VIEW.name(), Enums.Resource.JOB.name());
//        }

        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        logger.debug("Task get: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return taskCollection.nativeQuery().find(bson, qOptions).iterator();
    }

    private QueryOptions fixOptions(QueryOptions queryOptions) {
        QueryOptions options = new QueryOptions(queryOptions);

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

    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(long studyUid, Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(long studyUid, Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {

    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        Query queryCopy = new Query(query);

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
                        addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case RESOURCE_MANAGER_ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case STATUS_NAME:
                        // Convert the status to a positive status
                        queryCopy.put(queryParam.key(),
                                Status.getPositiveStatus(Enums.ExecutionStatus.STATUS_LIST, queryCopy.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case USER_ID:
                    case COMMAND_LINE:
                    case STATUS:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case ACTION:
                    case RESOURCE:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
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
}
