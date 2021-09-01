package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.MigrationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.MigrationConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.migration.MigrationRun;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MigrationMongoDBAdaptor extends MongoDBAdaptor implements MigrationDBAdaptor {

    private final MongoDBCollection migrationCollection;
    private final MigrationConverter migrationConverter;

    public MigrationMongoDBAdaptor(MongoDBCollection migrationCollection, Configuration configuration,
                                   MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(MigrationMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.migrationCollection = migrationCollection;
        this.migrationConverter = new MigrationConverter();
    }

    @Override
    public void upsert(MigrationRun migrationRun) throws CatalogDBException {
        Document migrationDocument = migrationConverter.convertToStorageType(migrationRun);
        Query query = new Query()
                .append(QueryParams.ID.key(), migrationRun.getId())
                .append(QueryParams.VERSION.key(), migrationRun.getVersion());
        Bson bsonQuery = parseQuery(query);
        DataResult update = migrationCollection.update(bsonQuery, new Document("$set", migrationDocument),
                new QueryOptions(MongoDBCollection.UPSERT, true));
        if (update.getNumUpdated() == 0 && update.getNumInserted() == 0) {
            throw new CatalogDBException("Could not update MigrationRun");
        }
    }

    @Override
    public OpenCGAResult<MigrationRun> get(Query query) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return new OpenCGAResult<>(migrationCollection.find(bsonQuery, migrationConverter, QueryOptions.empty()));
    }

    @Override
    public OpenCGAResult<MigrationRun> get(List<String> migrationRunIds) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), migrationRunIds);
        return get(query);
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        Query queryCopy = new Query(query);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            MigrationDBAdaptor.QueryParams queryParam = MigrationDBAdaptor.QueryParams.getParam(entry.getKey()) != null
                    ? MigrationDBAdaptor.QueryParams.getParam(entry.getKey())
                    : MigrationDBAdaptor.QueryParams.getParam(key);
            if (queryParam == null) {
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case ID:
                    case VERSION:
                    case START:
                    case END:
                    case PATCH:
                    case STATUS:
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
