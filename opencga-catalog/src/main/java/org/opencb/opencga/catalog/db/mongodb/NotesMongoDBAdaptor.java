package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.NotesDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.NotesConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.notes.Notes;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.filterObjectParams;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.filterStringParams;

public class NotesMongoDBAdaptor extends MongoDBAdaptor implements NotesDBAdaptor {

    private final MongoDBCollection notesCollection;
    private final MongoDBCollection archiveNotesCollection;
    private final MongoDBCollection deletedNotesCollection;
    private final NotesConverter notesConverter;
    private final VersionedMongoDBAdaptor versionedMongoDBAdaptor;

    public NotesMongoDBAdaptor(MongoDBCollection notesCollection, MongoDBCollection archiveNotesCollection,
                               MongoDBCollection deletedNotesCollection, Configuration configuration,
                               OrganizationMongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(NotesMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.notesCollection = notesCollection;
        this.archiveNotesCollection = archiveNotesCollection;
        this.deletedNotesCollection = deletedNotesCollection;
        this.notesConverter = new NotesConverter();
        this.versionedMongoDBAdaptor = new VersionedMongoDBAdaptor(notesCollection, archiveNotesCollection,
                deletedNotesCollection);
    }

    @Override
    public OpenCGAResult<Notes> insert(Notes notes)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting notes insert transaction for notes id '{}'", notes.getId());

            insert(clientSession, notes);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create notes {}: {}", notes.getId(), e.getMessage()));
    }

    Notes insert(ClientSession clientSession, Notes notes)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (StringUtils.isEmpty(notes.getId())) {
            throw new CatalogDBException("Missing notes id");
        }

        // Check it doesn't already exist a notes with same id
        Bson bson = Filters.eq(QueryParams.ID.key(), notes.getId());
        DataResult<Long> count = notesCollection.count(clientSession, bson);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Notes { id: '" + notes.getId() + "'} already exists.");
        }
        long notesUid = getNewUid(clientSession);
        notes.setUid(notesUid);
        if (StringUtils.isEmpty(notes.getUuid())) {
            notes.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.NOTES));
        }

        Document notesObject = notesConverter.convertToStorageType(notes);

        // Private parameters
        notesObject.put(PRIVATE_CREATION_DATE,
                StringUtils.isNotEmpty(notes.getCreationDate()) ? TimeUtils.toDate(notes.getCreationDate()) : TimeUtils.getDate());
        notesObject.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(notes.getModificationDate())
                ? TimeUtils.toDate(notes.getModificationDate()) : TimeUtils.getDate());

        logger.debug("Inserting notes '{}'...", notes.getId());
        versionedMongoDBAdaptor.insert(clientSession, notesObject);
        logger.debug("Notes '{}' successfully inserted", notes.getId());

        return notes;
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Notes> stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<Notes> get(Query query, QueryOptions options) throws CatalogDBException {
        return get(null, query, options);
    }

    OpenCGAResult<Notes> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Notes> dbIterator = iterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult update(long uid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> privateUpdate(clientSession, uid, parameters, queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update notes: {}", e.getMessage(), e);
            throw new CatalogDBException("Could not update notes: " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult<Notes> update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    OpenCGAResult<Object> privateUpdate(ClientSession clientSession, long notesUid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        Query tmpQuery = new Query(QueryParams.UID.key(), notesUid);
        Bson bsonQuery = parseQuery(tmpQuery);
        return versionedMongoDBAdaptor.update(clientSession, bsonQuery, () -> {
            UpdateDocument updateParams = parseAndValidateUpdateParams(clientSession, parameters, tmpQuery, queryOptions);
            Document notesUpdate = updateParams.toFinalUpdateDocument();

            if (notesUpdate.isEmpty()) {
                if (!parameters.isEmpty()) {
                    logger.error("Non-processed update parameters: {}", parameters.keySet());
                }
                throw new CatalogDBException("Nothing to be updated");
            }

            logger.debug("Notes update: query : {}, update: {}", bsonQuery.toBsonDocument(), notesUpdate.toBsonDocument());
            DataResult<?> result = notesCollection.update(clientSession, bsonQuery, notesUpdate, new QueryOptions("multi", true));

            if (result.getNumMatches() == 0) {
                throw new CatalogDBException("Notes '" + notesUid + "' not found");
            }

            List<Event> events = new ArrayList<>();
            if (result.getNumUpdated() == 0) {
                events.add(new Event(Event.Type.WARNING, "", "Notes was already updated"));
            }

            logger.debug("Notes '{}' successfully updated", notesUid);

            return endWrite(tmpStartTime, 1, 1, events);
        });
    }

    private UpdateDocument parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query tmpQuery,
                                                        QueryOptions queryOptions) {
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

        final String[] acceptedStringParams = {QueryParams.VALUE_TYPE.key()};
        filterStringParams(parameters, document.getSet(), acceptedStringParams);

        final String[] acceptedObjectParams = {QueryParams.VALUE.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
        // Phenotypes
        if (parameters.containsKey(QueryParams.TAGS.key())) {
            ParamUtils.BasicUpdateAction operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.TAGS.key(),
                    ParamUtils.BasicUpdateAction.ADD);
            String[] tagsParams = {QueryParams.TAGS.key()};
            switch (operation) {
                case SET:
                    filterObjectParams(parameters, document.getSet(), tagsParams);
                    break;
                case REMOVE:
                    filterObjectParams(parameters, document.getPull(), tagsParams);
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), tagsParams);
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(QueryParams.MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
                document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
            }
        }

        return document;
    }

    @Override
    public OpenCGAResult<Notes> delete(Notes notes)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> privateDelete(clientSession, notes));
        } catch (CatalogDBException e) {
            throw new CatalogDBException("Could not delete notes " + notes.getId() + ": " + e.getMessage(), e);
        }
    }

    private OpenCGAResult<Notes> privateDelete(ClientSession clientSession, Notes notes)
            throws CatalogDBException {
        long tmpStartTime = startQuery();

        logger.debug("Deleting notes {} ({})", notes.getId(), notes.getUid());

        // Delete notes
        Query notesQuery = new Query(QueryParams.UID.key(), notes.getUid());
        Bson bsonQuery = parseQuery(notesQuery);
        versionedMongoDBAdaptor.delete(clientSession, bsonQuery);
        logger.debug("Notes {}({}) deleted", notes.getId(), notes.getUid());
        return endWrite(tmpStartTime, 1, 0, 0, 1, Collections.emptyList());
    }

    @Override
    public OpenCGAResult<Notes> delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Notes> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult<Notes> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public DBIterator<Notes> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    DBIterator<Notes> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new CatalogMongoDBIterator<>(mongoCursor, notesConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException {
        Query finalQuery = new Query(query);

        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        Bson bson = parseQuery(finalQuery);
        MongoDBCollection collection = getQueryCollection(finalQuery, notesCollection, archiveNotesCollection,
                deletedNotesCollection);
        logger.debug("Notes query: {}", bson.toBsonDocument());
        return collection.iterator(clientSession, bson, null, null, qOptions);
    }

    @Override
    public OpenCGAResult<Notes> rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Notes> groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Notes> groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {

    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        Query queryCopy = new Query(query);

        if ("all".equalsIgnoreCase(queryCopy.getString(QueryParams.VERSION.key()))) {
            queryCopy.put(Constants.ALL_VERSIONS, true);
            queryCopy.remove(QueryParams.VERSION.key());
        }

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                if (Constants.ALL_VERSIONS.equals(entry.getKey())) {
                    continue;
                }
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ID:
                    case USER_ID:
                    case TAGS:
                    case VERSION:
                    case VALUE_TYPE:
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

        if (!andBsonList.isEmpty()) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }


}
