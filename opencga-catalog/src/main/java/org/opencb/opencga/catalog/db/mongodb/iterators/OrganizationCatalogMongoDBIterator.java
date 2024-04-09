package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MigrationMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class OrganizationCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    private final String user;

    private final QueryOptions options;

    private final Queue<Document> organizationListBuffer;
    private MigrationMongoDBAdaptor migrationDBAdaptor;

    private final Logger logger;


    public OrganizationCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                         GenericDocumentComplexConverter<E> converter, OrganizationMongoDBAdaptorFactory dbAdaptorFactory,
                                         QueryOptions options, String user) {
        super(mongoCursor, clientSession, converter, null);

        this.options = options != null ? new QueryOptions(options) : new QueryOptions();
        this.user = user;

        this.migrationDBAdaptor = dbAdaptorFactory.getMigrationDBAdaptor();

        this.organizationListBuffer = new LinkedList<>();
        this.logger = LoggerFactory.getLogger(OrganizationCatalogMongoDBIterator.class);
    }

    @Override
    public boolean hasNext() {
        if (organizationListBuffer.isEmpty()) {
            fetchNextBatch();
        }
        return !organizationListBuffer.isEmpty();
    }

    @Override
    public E next() {
        Document next = organizationListBuffer.remove();

        if (filter != null) {
            next = filter.apply(next);
        }

        if (converter != null) {
            return (E) converter.convertToDataModelType(next);
        } else {
            return (E) next;
        }
    }

    private void fetchNextBatch() {
        if (mongoCursor.hasNext()) {
            Document organizationDocument = mongoCursor.next();

            if (obtainMigrations()) {
                List<Document> migrationRuns = migrationDBAdaptor.nativeGet().getResults();
                Document internal = organizationDocument.get(OrganizationDBAdaptor.QueryParams.INTERNAL.key(), Document.class);
                if (internal == null) {
                    internal = new Document();
                    organizationDocument.put(OrganizationDBAdaptor.QueryParams.INTERNAL.key(), internal);
                }
                internal.put("migrationExecutions", migrationRuns);
            }

            // TODO: Remove this code after several releases (TASK-5923)
            recoverSecretKeyTask5923(organizationDocument);

            organizationListBuffer.add(organizationDocument);
        }
    }

    // TODO: Remove this method after several releases (TASK-5923)
    /**
     * This method should only be necessary in order to be able to run the migration for TASK-5923 because OpenCGA looks for the config.
     * This configuration was changed in TASK-5923, therefore, OpenCGA would be unable to initialise without it.
     * Once the migration is run, this code will do nothing, so it should be removed in the future.
     *
     * @param organizationDocument Organization document.
     */
    private void recoverSecretKeyTask5923(Document organizationDocument) {
        Document configuration = organizationDocument.get(OrganizationDBAdaptor.QueryParams.CONFIGURATION.key(), Document.class);
        if (configuration != null) {
            String secretKey = null;
            String algorithm = null;
            long expiration = 3600L;
            List<Document> authenticationOrigins = configuration.getList("authenticationOrigins", Document.class);
            if (CollectionUtils.isNotEmpty(authenticationOrigins)) {
                for (Document authenticationOrigin : authenticationOrigins) {
                    if (authenticationOrigin.getString("type").equals("OPENCGA")) {
                        secretKey = authenticationOrigin.getString("secretKey");
                        algorithm = authenticationOrigin.getString("algorithm");
                        Number expiration1 = authenticationOrigin.get("expiration", Number.class);
                        expiration = expiration1 != null ? expiration1.longValue() : expiration;

//                        authenticationOrigin.put("id", "OPENCGA");
                    }
                }
                if (StringUtils.isNotEmpty(secretKey)) {
                    Document token = configuration.get("token", Document.class);
                    if (token == null || StringUtils.isEmpty(token.getString("secretKey"))) {
                        token = new Document("secretKey", secretKey)
                                .append("algorithm", algorithm)
                                .append("expiration", expiration);
                        configuration.put("token", token);
                    }
                }
            }
        }
    }

    private boolean obtainMigrations() {
        if (options.containsKey(QueryOptions.INCLUDE)) {
            List<String> currentIncludeList = options.getAsStringList(QueryOptions.INCLUDE);
            for (String include : currentIncludeList) {
                if (include.equals(OrganizationDBAdaptor.QueryParams.INTERNAL.key())
                        || include.equals(OrganizationDBAdaptor.QueryParams.INTERNAL_MIGRATION_EXECUTIONS.key())) {
                    return true;
                }
            }
            return false;
        } else if (options.containsKey(QueryOptions.EXCLUDE)) {
            List<String> currentExcludeList = options.getAsStringList(QueryOptions.EXCLUDE);
            for (String exclude : currentExcludeList) {
                if (exclude.equals(OrganizationDBAdaptor.QueryParams.INTERNAL.key())
                        || exclude.equals(OrganizationDBAdaptor.QueryParams.INTERNAL_MIGRATION_EXECUTIONS.key())) {
                    return false;
                }
            }
            return true;
        }

        return true;
    }
}
