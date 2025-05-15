package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ExternalToolDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.WorkflowConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.WorkflowCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.externalTool.ExternalTool;
import org.opencb.opencga.core.models.externalTool.ExternalToolPermissions;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

public class ExternalToolMongoDBAdaptor extends CatalogMongoDBAdaptor implements ExternalToolDBAdaptor {

    private final MongoDBCollection workflowCollection;
    private final MongoDBCollection archiveWorkflowCollection;
    private final MongoDBCollection deleteWorkflowCollection;
    private final SnapshotVersionedMongoDBAdaptor versionedMongoDBAdaptor;
    private final WorkflowConverter workflowConverter;

    public ExternalToolMongoDBAdaptor(MongoDBCollection workflowCollection, MongoDBCollection archiveWorkflowCollection,
                                      MongoDBCollection deleteWorkflowCollection, Configuration configuration,
                                      OrganizationMongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(ExternalToolMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.workflowCollection = workflowCollection;
        this.archiveWorkflowCollection = archiveWorkflowCollection;
        this.deleteWorkflowCollection = deleteWorkflowCollection;
        this.versionedMongoDBAdaptor = new SnapshotVersionedMongoDBAdaptor(workflowCollection, archiveWorkflowCollection,
                deleteWorkflowCollection);
        this.workflowConverter = new WorkflowConverter();
    }

    public MongoDBCollection getCollection() {
        return workflowCollection;
    }

    public MongoDBCollection getArchiveCollection() {
        return archiveWorkflowCollection;
    }

    ExternalTool insert(ClientSession clientSession, long studyUid, ExternalTool externalTool) throws CatalogDBException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyUid);
        if (StringUtils.isEmpty(externalTool.getId())) {
            throw new CatalogDBException("Missing workflow id");
        }

        // Check the workflow does not exist
        Bson bson = Filters.and(
                Filters.eq(QueryParams.ID.key(), externalTool.getId()),
                Filters.eq(QueryParams.STUDY_UID.key(), studyUid)
        );
        DataResult<Long> count = workflowCollection.count(clientSession, bson);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Workflow { id: '" + externalTool.getId() + "'} already exists.");
        }

        long uid = getNewUid(clientSession);
        externalTool.setUid(uid);
        externalTool.setStudyUid(studyUid);
        externalTool.setRelease(dbAdaptorFactory.getCatalogStudyDBAdaptor().getCurrentRelease(clientSession, studyUid));

        Document workflowObject = workflowConverter.convertToStorageType(externalTool);

        workflowObject.put(PRIVATE_CREATION_DATE, StringUtils.isNotEmpty(externalTool.getCreationDate())
                ? TimeUtils.toDate(externalTool.getCreationDate()) : TimeUtils.getDate());
        workflowObject.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(externalTool.getModificationDate())
                ? TimeUtils.toDate(externalTool.getModificationDate()) : TimeUtils.getDate());

        logger.debug("Inserting workflow '{}' ({})...", externalTool.getId(), externalTool.getUid());
        versionedMongoDBAdaptor.insert(clientSession, workflowObject);
        logger.debug("Workflow '{}' successfully inserted", externalTool.getId());

        return externalTool;
    }

    @Override
    public OpenCGAResult<ExternalTool> insert(long studyUid, ExternalTool externalTool, QueryOptions options) throws CatalogException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting workflow insert transaction for workflow id '{}'", externalTool.getId());

            insert(clientSession, studyUid, externalTool);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create workflow {}: {}", externalTool.getId(), e.getMessage()));
    }

    @Override
    public OpenCGAResult<ExternalTool> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<ExternalTool> dbIterator = iterator(studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<ExternalTool> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<ExternalTool> dbIterator = iterator(query, options)) {
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
    public DBIterator<ExternalTool> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options, user);
        return new WorkflowCatalogMongoDBIterator<>(mongoCursor, null, workflowConverter, null, studyUid, user, options);
    }

    @Override
    public DBIterator<ExternalTool> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options, null);
        return new WorkflowCatalogMongoDBIterator<>(mongoCursor, null, workflowConverter, null, options);
    }

    @Override
    public DBIterator<Document> nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, queryOptions, null);
        return new WorkflowCatalogMongoDBIterator(mongoCursor, null, null, null, options);
    }

    @Override
    public DBIterator<Document> nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, queryOptions, user);
        return new WorkflowCatalogMongoDBIterator<>(mongoCursor, null, null, null, studyUid, user, options);
    }

    @Override
    public OpenCGAResult<Long> count(Query query, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        return new OpenCGAResult<>(workflowCollection.count(null, bson));
    }

    @Override
    public OpenCGAResult<Long> count(Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(workflowCollection.count(null, bson));
    }

    @Override
    public OpenCGAResult<ExternalTool> groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return null;
    }

    @Override
    public OpenCGAResult<ExternalTool> groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(workflowCollection, bsonQuery, field, ExternalToolDBAdaptor.QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult<ExternalTool> groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(workflowCollection, bsonQuery, fields, ExternalToolDBAdaptor.QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult<?> distinct(long studyUid, String field, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(ExternalToolDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);
        return new OpenCGAResult<>(workflowCollection.distinct(field, bson));
    }

    @Override
    public OpenCGAResult<?> distinct(long studyUid, List<String> fields, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        StopWatch stopWatch = StopWatch.createStarted();
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(ExternalToolDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        Set<String> results = new LinkedHashSet<>();
        for (String field : fields) {
            results.addAll(workflowCollection.distinct(field, bson, String.class).getResults());
        }

        return new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), Collections.emptyList(), results.size(),
                new ArrayList<>(results), -1);
    }

    @Override
    public OpenCGAResult<FacetField> facet(long studyUid, Query query, String facet, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, userId);
        return facet(workflowCollection, bson, facet);
    }

    @Override
    public OpenCGAResult<ExternalTool> stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<ExternalTool> update(long uid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), uid);
        return update(query, parameters, queryOptions);
    }

    @Override
    public OpenCGAResult<ExternalTool> update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> privateUpdate(clientSession, query, parameters, queryOptions));
        } catch (CatalogException e) {
            logger.error("Could not update workflows for query {}", query.toJson(), e);
            throw new CatalogDBException("Could not update workflows based on query " + query.toJson(), e);
        }
    }

    OpenCGAResult<ExternalTool> privateUpdate(ClientSession clientSession, Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();
        Bson bsonQuery = parseQuery(query);
        return versionedMongoDBAdaptor.update(clientSession, bsonQuery, (entrylist) -> {
            String workflowIds = entrylist.stream().map(w -> w.getString(QueryParams.ID.key())).collect(Collectors.joining(", "));

            UpdateDocument updateParams = parseAndValidateUpdateParams(parameters, queryOptions);
            Document workflowUpdate = updateParams.toFinalUpdateDocument();

            if (workflowUpdate.isEmpty()) {
                if (!parameters.isEmpty()) {
                    logger.error("Non-processed update parameters: {}", parameters.keySet());
                }
                throw new CatalogDBException("Nothing to be updated");
            }

            List<Event> events = new ArrayList<>();

            logger.debug("Workflow update: query : {}, update: {}", bsonQuery.toBsonDocument(), workflowUpdate.toBsonDocument());
            DataResult<?> result = workflowCollection.update(clientSession, bsonQuery, workflowUpdate,
                    new QueryOptions(MongoDBCollection.MULTI, true));

            if (result.getNumMatches() == 0) {
                throw new CatalogDBException("Workflow(s) '" + workflowIds + "' not found");
            }
            if (result.getNumUpdated() == 0) {
                events.add(new Event(Event.Type.WARNING, workflowIds, "Workflow(s) already updated"));
            }
            logger.debug("Workflow(s) '{}' successfully updated", workflowIds);


            return endWrite(tmpStartTime, result.getNumMatches(), result.getNumUpdated(), events);
        }, null, null);
    }

    private UpdateDocument parseAndValidateUpdateParams(ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        if (parameters.containsKey(QueryParams.ID.key())) {
            throw new CatalogDBException("It is not allowed to update the 'id' of a workflow");
        }

        UpdateDocument document = new UpdateDocument();

        if (StringUtils.isNotEmpty(parameters.getString(QueryParams.CREATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.CREATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.CREATION_DATE.key(), time);
            document.getSet().put(PRIVATE_CREATION_DATE, date);
        }
        if (StringUtils.isNotEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.MODIFICATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }

        final String[] acceptedBooleanParams = {QueryParams.DRAFT.key()};
        filterBooleanParams(parameters, document.getSet(), acceptedBooleanParams);

        final String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.DESCRIPTION.key(), QueryParams.SCOPE.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        final String[] acceptedListParams = {QueryParams.DOCKER.key(), QueryParams.WORKFLOW_MANAGER.key(),
                QueryParams.WORKFLOW_SCRIPTS.key(), QueryParams.WORKFLOW_REPOSITORY.key(), QueryParams.TAGS.key(),
                QueryParams.VARIABLES.key(), QueryParams.MINIMUM_REQUIREMENTS.key()};
        filterObjectParams(parameters, document.getSet(), acceptedListParams);

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(QueryParams.MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
                document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
            }
            document.getSet().put(INTERNAL_LAST_MODIFIED, time);
        }

        return document;
    }

    @Override
    public OpenCGAResult<ExternalTool> delete(ExternalTool externalTool)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            Query query = new Query()
                    .append(QueryParams.UID.key(), externalTool.getUid())
                    .append(QueryParams.STUDY_UID.key(), externalTool.getStudyUid());
            OpenCGAResult<Document> result = nativeGet(query, new QueryOptions());
            if (result.getNumResults() == 0) {
                throw new CatalogDBException("Could not find workflow " + externalTool.getId() + " with uid " + externalTool.getUid());
            }
            return runTransaction(clientSession -> privateDelete(clientSession, result.first()));
        } catch (CatalogException e) {
            logger.error("Could not delete workflow {}: {}", externalTool.getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete workflow " + externalTool.getId() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public OpenCGAResult<ExternalTool> delete(Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try (DBIterator<Document> iterator = nativeIterator(query, new QueryOptions())) {
            OpenCGAResult<ExternalTool> result = OpenCGAResult.empty(ExternalTool.class);
            while (iterator.hasNext()) {
                Document workflow = iterator.next();
                String workflowId = workflow.getString(QueryParams.ID.key());

                try {
                    result.append(runTransaction(clientSession -> privateDelete(clientSession, workflow)));
                } catch (CatalogException e) {
                    logger.error("Could not delete workflow {}: {}", workflowId, e.getMessage(), e);
                    result.getEvents().add(new Event(Event.Type.ERROR, workflowId, e.getMessage()));
                    result.setNumMatches(result.getNumMatches() + 1);
                }
            }
            return result;
        }
    }

    OpenCGAResult<ExternalTool> privateDelete(ClientSession clientSession, Document workflowDocument)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        String workflowId = workflowDocument.getString(QueryParams.ID.key());
        long workflowUid = workflowDocument.getLong(PRIVATE_UID);
        long studyUid = workflowDocument.getLong(PRIVATE_STUDY_UID);

        logger.debug("Deleting workflow {} ({})", workflowId, workflowUid);
        // Delete workflow
        Query workflowQuery = new Query()
                .append(QueryParams.UID.key(), workflowUid)
                .append(QueryParams.STUDY_UID.key(), studyUid);
        Bson bsonQuery = parseQuery(workflowQuery);
        versionedMongoDBAdaptor.delete(clientSession, bsonQuery);
        logger.debug("Workflow {}({}) deleted", workflowId, workflowUid);
        return endWrite(tmpStartTime, 1, 0, 0, 1, Collections.emptyList());
    }

    @Override
    public OpenCGAResult<ExternalTool> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult<ExternalTool> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult<ExternalTool> rank(Query query, String field, int numResults, boolean asc)
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
        MongoDBCollection collection = getQueryCollection(finalQuery, workflowCollection, archiveWorkflowCollection,
                deleteWorkflowCollection);
        logger.debug("Nextflow query: {}", bson.toBsonDocument());
        return collection.iterator(clientSession, bson, null, null, qOptions);
    }


    private Bson parseQuery(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, null);
    }

    private Bson parseQuery(Query query, String user) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, user);
    }

    protected Bson parseQuery(Query query, Document extraQuery)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, extraQuery, null);
    }

    protected Bson parseQuery(Query query, Document extraQuery, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        List<Bson> andBsonList = new ArrayList<>();

        if (query.containsKey(QueryParams.STUDY_UID.key())
                && (StringUtils.isNotEmpty(user) || query.containsKey(ParamConstants.ACL_PARAM))) {
            Document studyDocument = getStudyDocument(null, query.getLong(QueryParams.STUDY_UID.key()));
            boolean simplifyPermissions = simplifyPermissions();

            if (query.containsKey(ParamConstants.ACL_PARAM)) {
                andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.EXTERNAL_TOOL, user,
                        simplifyPermissions));
            } else {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, ExternalToolPermissions.VIEW.name(),
                            Enums.Resource.EXTERNAL_TOOL, simplifyPermissions));
            }

            query.remove(ParamConstants.ACL_PARAM);
        }

        Query queryCopy = new Query(query);
//        queryCopy.remove(WorkflowDBAdaptor.QueryParams.DELETED.key());

        if ("all".equalsIgnoreCase(queryCopy.getString(ExternalToolDBAdaptor.QueryParams.VERSION.key()))) {
            queryCopy.put(Constants.ALL_VERSIONS, true);
            queryCopy.remove(ExternalToolDBAdaptor.QueryParams.VERSION.key());
        }

        boolean uidVersionQueryFlag = versionedMongoDBAdaptor.generateUidVersionQuery(queryCopy, andBsonList);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            ExternalToolDBAdaptor.QueryParams queryParam = ExternalToolDBAdaptor.QueryParams.getParam(entry.getKey()) != null
                    ? ExternalToolDBAdaptor.QueryParams.getParam(entry.getKey())
                    : ExternalToolDBAdaptor.QueryParams.getParam(key);
            if (queryParam == null) {
                if (Constants.ALL_VERSIONS.equals(entry.getKey())) {
                    continue;
                }
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case UID:
                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case SNAPSHOT:
                        addAutoOrQuery(RELEASE_FROM_VERSION, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
                        addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case NAME:
                    case WORKFLOW_REPOSITORY_NAME:
                    case DOCKER_NAME:
                    case TAGS:
                    case RELEASE:
                    case VERSION:
                    case INTERNAL_REGISTRATION_USER_ID:
                    case TYPE:
                    case SCOPE:
                    case DRAFT:
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

        // If the user doesn't look for a concrete version...
        if (!uidVersionQueryFlag && !queryCopy.getBoolean(Constants.ALL_VERSIONS)
                && !queryCopy.containsKey(ExternalToolDBAdaptor.QueryParams.VERSION.key())
                && queryCopy.containsKey(ExternalToolDBAdaptor.QueryParams.SNAPSHOT.key())) {
            // If the user looks for anything from some release, we will try to find the latest from the release (snapshot)
            andBsonList.add(Filters.eq(LAST_OF_RELEASE, true));
        }

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }
}
