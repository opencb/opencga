package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MigrationMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.ProjectMongoDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class OrganizationCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    private final String user;

    private final QueryOptions options;
    private final QueryOptions projectOptions;

    private final Queue<Document> organizationListBuffer;
    private final MigrationMongoDBAdaptor migrationDBAdaptor;
    private final ProjectMongoDBAdaptor projectMongoDBAdaptor;

    private final Logger logger;


    public OrganizationCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                         GenericDocumentComplexConverter<E> converter, OrganizationMongoDBAdaptorFactory dbAdaptorFactory,
                                         QueryOptions options, String user) {
        super(mongoCursor, clientSession, converter, null);

        this.options = options != null ? new QueryOptions(options) : new QueryOptions();
        this.projectOptions = createInnerQueryOptionsForVersionedEntity(this.options, OrganizationDBAdaptor.QueryParams.PROJECTS.key(),
                true);
        this.user = user;

        this.migrationDBAdaptor = dbAdaptorFactory.getMigrationDBAdaptor();
        this.projectMongoDBAdaptor = dbAdaptorFactory.getCatalogProjectDBAdaptor();

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

            if (includeField(options, OrganizationDBAdaptor.QueryParams.PROJECTS.key())) {
                OpenCGAResult<Document> openCGAResult = null;
                try {
                    openCGAResult = projectMongoDBAdaptor.nativeGet(clientSession, new Query(), projectOptions);
                } catch (CatalogDBException e) {
                    logger.warn("Could not fetch projects for organization.");
                }
                organizationDocument.put(OrganizationDBAdaptor.QueryParams.PROJECTS.key(), openCGAResult.getResults());
            }

            organizationListBuffer.add(organizationDocument);
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
