package org.opencb.opencga.catalog.db.mongodb;

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
import org.opencb.opencga.catalog.db.api.SettingsDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.SettingsConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.settings.Settings;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class SettingsMongoDBAdaptor extends MongoDBAdaptor implements SettingsDBAdaptor {

    private final MongoDBCollection settingsCollection;
    private final MongoDBCollection archiveSettingsCollection;
    private final MongoDBCollection deletedSettingsCollection;
    private final SettingsConverter settingsConverter;
    private final VersionedMongoDBAdaptor versionedMongoDBAdaptor;

    public SettingsMongoDBAdaptor(MongoDBCollection settingsCollection, MongoDBCollection archiveSettingsCollection,
                                MongoDBCollection deletedSettingsCollection, Configuration configuration,
                                  OrganizationMongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(SettingsMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.settingsCollection = settingsCollection;
        this.archiveSettingsCollection = archiveSettingsCollection;
        this.deletedSettingsCollection = deletedSettingsCollection;
        this.settingsConverter = new SettingsConverter();
        this.versionedMongoDBAdaptor = new VersionedMongoDBAdaptor(settingsCollection, archiveSettingsCollection,
                deletedSettingsCollection);
    }

    @Override
    public OpenCGAResult<Settings> insert(Settings settings)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting settings insert transaction for settings id '{}'", settings.getId());

            insert(clientSession, settings);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create settings {}: {}", settings.getId(), e.getMessage()));
    }

    Settings insert(ClientSession clientSession, Settings settings)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (StringUtils.isEmpty(settings.getId())) {
            throw new CatalogDBException("Missing settings id");
        }

        // Check it doesn't already exist a settings with same id
        Bson bson = Filters.eq(QueryParams.ID.key(), settings.getId());
        DataResult<Long> count = settingsCollection.count(clientSession, bson);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Settings { id: '" + settings.getId() + "'} already exists.");
        }

        Document settingsObject = settingsConverter.convertToStorageType(settings);

        // Private parameters
        settingsObject.put(PRIVATE_CREATION_DATE,
                StringUtils.isNotEmpty(settings.getCreationDate()) ? TimeUtils.toDate(settings.getCreationDate()) : TimeUtils.getDate());
        settingsObject.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(settings.getModificationDate())
                ? TimeUtils.toDate(settings.getModificationDate()) : TimeUtils.getDate());

        logger.debug("Inserting settings '{}'...", settings.getId());
        versionedMongoDBAdaptor.insert(clientSession, settingsObject);
        logger.debug("Settings '{}' successfully inserted", settings.getId());

        return settings;
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Settings> stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<Settings> get(Query query, QueryOptions options) throws CatalogDBException {
        return get(null, query, options);
    }

    OpenCGAResult<Settings> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Settings> dbIterator = iterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Settings> update(long id, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Settings> update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Settings> delete(Settings id) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Settings> delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Settings> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult<Settings> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public DBIterator<Settings> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    DBIterator<Settings> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new CatalogMongoDBIterator<>(mongoCursor, settingsConverter);
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
        MongoDBCollection collection = getQueryCollection(finalQuery, settingsCollection, archiveSettingsCollection,
                deletedSettingsCollection);
        logger.debug("Settings query: {}", bson.toBsonDocument());
        return collection.iterator(clientSession, bson, null, null, qOptions);
    }

    @Override
    public OpenCGAResult<Settings> rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Settings> groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<Settings> groupBy(Query query, List<String> fields, QueryOptions options)
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
                    case ID:
                    case TAGS:
                    case VERSION:
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
