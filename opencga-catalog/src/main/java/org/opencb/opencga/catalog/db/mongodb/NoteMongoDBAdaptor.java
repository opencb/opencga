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
import org.opencb.opencga.catalog.db.api.NoteDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.NoteConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.notes.Note;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

public class NoteMongoDBAdaptor extends CatalogMongoDBAdaptor implements NoteDBAdaptor {

    private final MongoDBCollection noteCollection;
    private final MongoDBCollection archiveNoteCollection;
    private final MongoDBCollection deletedNoteCollection;
    private final NoteConverter noteConverter;
    private final VersionedMongoDBAdaptor versionedMongoDBAdaptor;

    public NoteMongoDBAdaptor(MongoDBCollection noteCollection, MongoDBCollection archiveNoteCollection,
                              MongoDBCollection deletedNoteCollection, Configuration configuration,
                              OrganizationMongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(NoteMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.noteCollection = noteCollection;
        this.archiveNoteCollection = archiveNoteCollection;
        this.deletedNoteCollection = deletedNoteCollection;
        this.noteConverter = new NoteConverter();
        this.versionedMongoDBAdaptor = new VersionedMongoDBAdaptor(noteCollection, archiveNoteCollection,
                deletedNoteCollection);
    }

    @Override
    public OpenCGAResult<Note> insert(Note note)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting note insert transaction for note id '{}'", note.getId());

            insert(clientSession, note);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create note {}: {}", note.getId(), e.getMessage()));
    }

    Note insert(ClientSession clientSession, Note note)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (StringUtils.isEmpty(note.getId())) {
            throw new CatalogDBException("Missing note id");
        }

        if (note.getStudyUid() <= 0) {
            note.setStudyUid(-1L);
        }
        // Check it doesn't already exist a note with same id and study
        Bson bson = Filters.and(
                Filters.eq(QueryParams.ID.key(), note.getId()),
                Filters.eq(QueryParams.STUDY_UID.key(), note.getStudyUid())
        );
        DataResult<Long> count = noteCollection.count(clientSession, bson);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Note { id: '" + note.getId() + "'} already exists.");
        }
        long noteUid = getNewUid(clientSession);
        note.setUid(noteUid);
        if (StringUtils.isEmpty(note.getUuid())) {
            note.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.NOTES));
        }

        Document noteObject = noteConverter.convertToStorageType(note);

        // Private parameters
        noteObject.put(PRIVATE_CREATION_DATE,
                StringUtils.isNotEmpty(note.getCreationDate()) ? TimeUtils.toDate(note.getCreationDate()) : TimeUtils.getDate());
        noteObject.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(note.getModificationDate())
                ? TimeUtils.toDate(note.getModificationDate()) : TimeUtils.getDate());

        logger.debug("Inserting note '{}'...", note.getId());
        versionedMongoDBAdaptor.insert(clientSession, noteObject);
        logger.debug("Note '{}' successfully inserted", note.getId());

        return note;
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Note> stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<Note> get(Query query, QueryOptions options) throws CatalogDBException {
        return get(null, query, options);
    }

    OpenCGAResult<Note> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Note> dbIterator = iterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeGet(null, query, options);
    }


    public OpenCGAResult<Document> nativeGet(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult update(long uid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> privateUpdate(clientSession, uid, parameters, queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update note: {}", e.getMessage(), e);
            throw new CatalogDBException("Could not update note: " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult<Note> update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    OpenCGAResult<Object> privateUpdate(ClientSession clientSession, long noteUid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        Query tmpQuery = new Query(QueryParams.UID.key(), noteUid);
        Bson bsonQuery = parseQuery(tmpQuery);
        List<String> includeFields = Collections.singletonList(QueryParams.VALUE_TYPE.key());
        return versionedMongoDBAdaptor.update(clientSession, bsonQuery, includeFields, (noteList) -> {
            Document note = noteList.get(0);
            UpdateDocument updateParams = parseAndValidateUpdateParams(note, parameters, queryOptions);
            Document noteUpdate = updateParams.toFinalUpdateDocument();

            if (noteUpdate.isEmpty()) {
                if (!parameters.isEmpty()) {
                    logger.error("Non-processed update parameters: {}", parameters.keySet());
                }
                throw new CatalogDBException("Nothing to be updated");
            }

            logger.debug("Note update: query : {}, update: {}", bsonQuery.toBsonDocument(), noteUpdate.toBsonDocument());
            DataResult<?> result = noteCollection.update(clientSession, bsonQuery, noteUpdate, new QueryOptions("multi", true));

            if (result.getNumMatches() == 0) {
                throw new CatalogDBException("Note '" + noteUid + "' not found");
            }

            List<Event> events = new ArrayList<>();
            if (result.getNumUpdated() == 0) {
                events.add(new Event(Event.Type.WARNING, "", "Note was already updated"));
            }

            logger.debug("Note '{}' successfully updated", noteUid);

            return endWrite(tmpStartTime, 1, 1, events);
        });
    }

    private UpdateDocument parseAndValidateUpdateParams(Document note, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

//        if (StringUtils.isNotEmpty(parameters.getString(QueryParams.CREATION_DATE.key()))) {
//            String time = parameters.getString(QueryParams.CREATION_DATE.key());
//            Date date = TimeUtils.toDate(time);
//            document.getSet().put(QueryParams.CREATION_DATE.key(), time);
//            document.getSet().put(PRIVATE_CREATION_DATE, date);
//        }
//        if (StringUtils.isNotEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
//            String time = parameters.getString(QueryParams.MODIFICATION_DATE.key());
//            Date date = TimeUtils.toDate(time);
//            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
//            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
//        }

        final String[] acceptedStringParams = {QueryParams.USER_ID.key(), QueryParams.VISIBILITY.key()};
        filterStringParams(parameters, document.getSet(), acceptedStringParams);

        Object value = parameters.get(QueryParams.VALUE.key());
        if (value != null) {
            String valueTypeStr = note.getString(QueryParams.VALUE_TYPE.key());
            Note.Type type = Note.Type.valueOf(valueTypeStr);
            if (type.equals(Note.Type.OBJECT)) {
                final String[] acceptedValueParams = {QueryParams.VALUE.key()};
                filterObjectParams(parameters, document.getSet(), acceptedValueParams);
            } else {
                document.getSet().put(QueryParams.VALUE.key(), value);
            }
        }

        Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
        // Tags
        if (parameters.containsKey(QueryParams.TAGS.key())) {
            ParamUtils.BasicUpdateAction operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.TAGS.key(),
                    ParamUtils.BasicUpdateAction.ADD);
            String[] tagsParams = {QueryParams.TAGS.key()};
            switch (operation) {
                case SET:
                    filterStringListParams(parameters, document.getSet(), tagsParams);
                    break;
                case REMOVE:
                    filterStringListParams(parameters, document.getPullAll(), tagsParams);
                    break;
                case ADD:
                    filterStringListParams(parameters, document.getAddToSet(), tagsParams);
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
    public OpenCGAResult<Note> delete(Note note)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> privateDelete(clientSession, note));
        } catch (CatalogDBException e) {
            throw new CatalogDBException("Could not delete note " + note.getId() + ": " + e.getMessage(), e);
        }
    }

    private OpenCGAResult<Note> privateDelete(ClientSession clientSession, Note note)
            throws CatalogDBException {
        long tmpStartTime = startQuery();

        logger.debug("Deleting note {} ({})", note.getId(), note.getUid());

        // Delete note
        Query noteQuery = new Query(QueryParams.UID.key(), note.getUid());
        Bson bsonQuery = parseQuery(noteQuery);
        versionedMongoDBAdaptor.delete(clientSession, bsonQuery);
        logger.debug("Note {}({}) deleted", note.getId(), note.getUid());
        return endWrite(tmpStartTime, 1, 0, 0, 1, Collections.emptyList());
    }

    @Override
    public OpenCGAResult<Note> delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Note> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult<Note> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public DBIterator<Note> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    DBIterator<Note> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new CatalogMongoDBIterator<>(mongoCursor, noteConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeIterator(null, query, options);
    }

    DBIterator<Document> nativeIterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new CatalogMongoDBIterator<>(mongoCursor, clientSession, null, null);
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
        MongoDBCollection collection = getQueryCollection(finalQuery, noteCollection, archiveNoteCollection,
                deletedNoteCollection);
        logger.debug("Note query: {}", bson.toBsonDocument());
        return collection.iterator(clientSession, bson, null, null, qOptions);
    }

    @Override
    public OpenCGAResult<Note> rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Note> groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Note> groupBy(Query query, List<String> fields, QueryOptions options)
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
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case SCOPE:
                    case USER_ID:
                    case TAGS:
                    case VERSION:
                    case VISIBILITY:
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
