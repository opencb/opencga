package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.PipelineDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.PipelineConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.job.Pipeline;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

public class PipelineMongoDBAdaptor extends MongoDBAdaptor implements PipelineDBAdaptor {

    private final MongoDBCollection pipelineCollection;
    private final MongoDBCollection lastPipelineCollection;
    private final MongoDBCollection deletedPipelineCollection;
    private PipelineConverter pipelineConverter;

    private static final String PRIVATE_STUDY_UIDS = "_studyUids";

    public PipelineMongoDBAdaptor(MongoDBCollection pipelineCollection, MongoDBCollection lastPipelineCollection,
                                  MongoDBCollection deletedPipelineCollection, Configuration configuration,
                                  MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(PipelineMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.pipelineCollection = pipelineCollection;
        this.lastPipelineCollection = lastPipelineCollection;
        this.deletedPipelineCollection = deletedPipelineCollection;
        this.pipelineConverter = new PipelineConverter();
    }

    public MongoDBCollection getLastPipelineCollection() {
        return lastPipelineCollection;
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> pipeline, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(pipeline, "pipeline");
        return new OpenCGAResult(lastPipelineCollection.insert(document, null));
    }

    @Override
    public OpenCGAResult insert(long studyUid, Pipeline pipeline, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> {
                long tmpStartTime = startQuery();
                logger.debug("Starting pipeline insert transaction for id '{}' on studyUid '{}'", pipeline.getId(), studyUid);

                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyUid);
                insert(clientSession, studyUid, pipeline);
                return endWrite(tmpStartTime, 1, 1, 0, 0, null);
            });
        } catch (Exception e) {
            logger.error("Could not insert pipeline {}: {}", pipeline.getId(), e.getMessage());
            throw e;
        }
    }

    long insert(ClientSession clientSession, long studyId, Pipeline pipeline) throws CatalogDBException {
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(PipelineDBAdaptor.QueryParams.ID.key(), pipeline.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_UID, studyId));

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = lastPipelineCollection.count(clientSession, bson);

        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Pipeline { id: '" + pipeline.getId() + "'} already exists.");
        }

        long pipelineUid = getNewUid();
        pipeline.setUid(pipelineUid);
        pipeline.setStudyUid(studyId);
        if (StringUtils.isEmpty(pipeline.getUuid())) {
            pipeline.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.PIPELINE));
        }
        if (StringUtils.isEmpty(pipeline.getCreationDate())) {
            pipeline.setCreationDate(TimeUtils.getTime());
        }

        Document pipelineObject = pipelineConverter.convertToStorageType(pipeline);
        pipelineObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(pipeline.getCreationDate()));
        pipelineObject.put(PRIVATE_MODIFICATION_DATE, pipelineObject.get(PRIVATE_CREATION_DATE));
        pipelineObject.put(LAST_OF_VERSION, true);

        logger.debug("Inserting pipeline '{}' ({})...", pipeline.getId(), pipeline.getUid());
        pipelineCollection.insert(clientSession, pipelineObject, null);
        lastPipelineCollection.insert(clientSession, pipelineObject, null);
        logger.debug("Pipeline '{}' successfully inserted", pipeline.getId());
        return pipelineUid;
    }

    @Override
    public OpenCGAResult<Pipeline> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Pipeline> dbIterator = iterator(studyUid, query, options, user)) {
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
    public DBIterator<Pipeline> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options, user);
        return new CatalogMongoDBIterator<>(mongoCursor, pipelineConverter);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, queryOptions, user);
        return new CatalogMongoDBIterator(mongoCursor, pipelineConverter);
    }

    @Override
    public OpenCGAResult<Long> count(Query query, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        logger.debug("Execution count: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return new OpenCGAResult<>(lastPipelineCollection.count(bson));
    }

    @Override
    public OpenCGAResult<Pipeline> groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        throw new NotImplementedException("Method not implemented");
    }

    @Override
    public <T> OpenCGAResult<T> distinct(long studyUid, String field, Query query, String userId, Class<T> clazz)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Method not implemented");
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonDocument = parseQuery(query);
        return new OpenCGAResult<>(lastPipelineCollection.count(bsonDocument));
    }

    @Override
    public OpenCGAResult<Pipeline> stats(Query query) {
        throw new NotImplementedException("Method not implemented");
    }

    @Override
    public OpenCGAResult<Pipeline> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Pipeline> dbIterator = iterator(query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeGet(null, query, options);
    }

    public OpenCGAResult nativeGet(ClientSession session, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(session, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Pipeline> update(long uid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> {
                Query query = new Query(QueryParams.UID.key(), uid);
                OpenCGAResult<Document> documentResult = nativeGet(clientSession, query, QueryOptions.empty());
                if (documentResult.getNumResults() == 0) {
                    throw new CatalogDBException("Could not update pipeline. Pipeline uid '" + uid + "' not found.");
                }

                return privateUpdate(clientSession, documentResult.first(), parameters, queryOptions);
            });
        } catch (CatalogDBException e) {
            logger.error("Could not update pipeline {}: {}", uid, e.getMessage(), e);
            throw new CatalogDBException("Could not update pipeline " + uid + ": " + e.getMessage(), e.getCause());
        }
    }

    private OpenCGAResult<Pipeline> privateUpdate(ClientSession clientSession, Document pipelineDocument, ObjectMap parameters,
                                                  QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();
        String pipelineId = pipelineDocument.getString(QueryParams.ID.key());
        long pipelineUid = pipelineDocument.getLong(QueryParams.UID.key());
        long studyUid = pipelineDocument.getLong(QueryParams.STUDY_UID.key());

        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.UID.key(), pipelineUid)
                .append(LAST_OF_VERSION, true);

        createNewVersion(clientSession, pipelineCollection, lastPipelineCollection, pipelineDocument);

        UpdateDocument updateParams = parseAndValidateUpdateParams(clientSession, parameters, queryOptions);
        Document pipelineUpdate = updateParams.toFinalUpdateDocument();

        if (pipelineUpdate.isEmpty()) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("Nothing to be updated");
        }

        List<Event> events = new ArrayList<>();

        Bson finalQuery = parseQuery(tmpQuery);
        logger.debug("Pipeline update: query : {}, update: {}",
                finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                pipelineUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        DataResult updateResult = pipelineCollection.update(clientSession, finalQuery, pipelineUpdate, QueryOptions.empty());
        if (updateResult.getNumMatches() == 0) {
            throw new CatalogDBException("Could not update pipeline '" + pipelineId + "'. Pipeline not found.");
        }
        updateResult = lastPipelineCollection.update(clientSession, finalQuery, pipelineUpdate, QueryOptions.empty());
        if (updateResult.getNumMatches() == 0) {
            throw new CatalogDBException("Could not update pipeline '" + pipelineId + "'. Pipeline not found.");
        }
        if (updateResult.getNumUpdated() == 0) {
            events.add(new Event(Event.Type.WARNING, pipelineId, "Pipeline was already updated"));
        }
        logger.debug("Pipeline {} successfully updated", pipelineId);

        return endWrite(tmpStartTime, 1, 1, events);
    }

    private UpdateDocument parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, QueryOptions queryOptions) {
        UpdateDocument document = new UpdateDocument();

        String[] acceptedParams = {QueryParams.DESCRIPTION.key(), QueryParams.CREATION_DATE.key(), QueryParams.MODIFICATION_DATE.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

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

        String[] acceptedBooleanParams = {QueryParams.DISABLED.key()};
        filterBooleanParams(parameters, document.getSet(), acceptedBooleanParams);

        String[] acceptedMapParams = {QueryParams.PARAMS.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        String[] acceptedObjectParams = {QueryParams.CONFIG.key(), QueryParams.JOBS.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_NAME.key())) {
            document.getSet().put(QueryParams.INTERNAL_STATUS_NAME.key(), parameters.get(QueryParams.INTERNAL_STATUS_NAME.key()));
            document.getSet().put(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                document.getSet().put(IndividualDBAdaptor.QueryParams.MODIFICATION_DATE.key(), time);
                document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
            }
            document.getSet().put(INTERNAL_LAST_MODIFIED, time);
        }

        return document;
    }

    @Override
    public OpenCGAResult<Pipeline> update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Method not implemented");
    }

    @Override
    public DBIterator<Pipeline> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options);
        return new CatalogMongoDBIterator<>(mongoCursor, pipelineConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeIterator(null, query, options);
    }

    public DBIterator nativeIterator(ClientSession session, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(session, query, queryOptions);
        return new CatalogMongoDBIterator(mongoCursor, pipelineConverter);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Objects.requireNonNull(action);
        try (DBIterator<Pipeline> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(clientSession, query, options, null);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession session, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        Bson bson = parseQuery(query, user);

        logger.debug("Pipeline get: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        if (!query.getBoolean(ParamConstants.DELETED_PARAM)) {
            if (isQueryingOldVersions(query)) {
                return pipelineCollection.iterator(session, bson, null, null, qOptions);
            } else {
                return lastPipelineCollection.iterator(session, bson, null, null, qOptions);
            }
        } else {
            return deletedPipelineCollection.iterator(session, bson, null, null, qOptions);
        }
    }

    private boolean isQueryingOldVersions(Query query) {
        return query.containsKey(QueryParams.VERSION.key());
    }


    private Bson parseQuery(Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, null);
    }

    private Bson parseQuery(Query query, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, user);
    }

    protected Bson parseQuery(Query query, Document extraQuery)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, extraQuery, null);
    }

    private Bson parseQuery(Query query, Document extraQuery, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        List<Bson> andBsonList = new ArrayList<>();

//        if (query.containsKey(MongoDBAdaptor.PRIVATE_STUDY_UID)
//                && (StringUtils.isNotEmpty(user) || query.containsKey(ParamConstants.ACL_PARAM))) {
//            Document studyDocument = getStudyDocument(null, query.getLong(MongoDBAdaptor.PRIVATE_STUDY_UID));
//
//            // Get the document query needed to check the permissions as well
//            andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, JobAclEntry.JobPermissions.VIEW.name(),
//                    Enums.Resource.EXECUTION, configuration));
//
//            andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.EXECUTION, user,
//                    configuration));
//
//            query.remove(ParamConstants.ACL_PARAM);
//        }

        if (query.containsKey(LAST_OF_VERSION)) {
            addAutoOrQuery(LAST_OF_VERSION, LAST_OF_VERSION, query, QueryParam.Type.BOOLEAN, andBsonList);
        }

        Query queryCopy = new Query(query);
        queryCopy.remove(ParamConstants.DELETED_PARAM);
        queryCopy.remove(LAST_OF_VERSION);

//        fixComplexQueryParam(PipelineDBAdaptor.QueryParams.ATTRIBUTES.key(), queryCopy);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            PipelineDBAdaptor.QueryParams queryParam = (PipelineDBAdaptor.QueryParams.getParam(entry.getKey()) != null)
                    ? PipelineDBAdaptor.QueryParams.getParam(entry.getKey())
                    : PipelineDBAdaptor.QueryParams.getParam(key);
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
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
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

    @Override
    public OpenCGAResult<Pipeline> delete(Pipeline id) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Method not implemented");
    }

    @Override
    public OpenCGAResult<Pipeline> delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Method not implemented");
    }

    @Override
    public OpenCGAResult<Pipeline> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Method not implemented");
    }

    @Override
    public OpenCGAResult<Pipeline> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Method not implemented");
    }

    @Override
    public OpenCGAResult<Pipeline> rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Method not implemented");
    }

    @Override
    public OpenCGAResult<Pipeline> groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Method not implemented");
    }

    @Override
    public OpenCGAResult<Pipeline> groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Method not implemented");
    }

}
