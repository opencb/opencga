package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.NoteDBAdaptor;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MigrationMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.NoteMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.ProjectMongoDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.models.notes.Note;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class OrganizationCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    private final String user;

    private final QueryOptions options;
    private final QueryOptions projectOptions;

    private final Queue<Document> organizationListBuffer;
    private final MigrationMongoDBAdaptor migrationDBAdaptor;
    private final ProjectMongoDBAdaptor projectMongoDBAdaptor;
    private final NoteMongoDBAdaptor noteMongoDBAdaptor;

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
        this.noteMongoDBAdaptor = dbAdaptorFactory.getCatalogNotesDBAdaptor();
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

            if (!options.getBoolean(NATIVE_QUERY)) {
                List<String> migrationFields = Arrays.asList(OrganizationDBAdaptor.QueryParams.INTERNAL.key(),
                        OrganizationDBAdaptor.QueryParams.INTERNAL_MIGRATION_EXECUTIONS.key());
                if (includeField(options, migrationFields)) {
                    List<Document> migrationRuns = migrationDBAdaptor.nativeGet().getResults();
                    Document internal = organizationDocument.get(OrganizationDBAdaptor.QueryParams.INTERNAL.key(), Document.class);
                    if (internal == null) {
                        internal = new Document();
                        organizationDocument.put(OrganizationDBAdaptor.QueryParams.INTERNAL.key(), internal);
                    }
                    internal.put("migrationExecutions", migrationRuns);
                }

                List<String> noteField = Collections.singletonList(OrganizationDBAdaptor.QueryParams.NOTES.key());
                if (includeField(options, noteField)) {
                    Query query = new Query(NoteDBAdaptor.QueryParams.SCOPE.key(), Note.Scope.ORGANIZATION.name());
                    if (!options.getBoolean(OrganizationDBAdaptor.IS_ORGANIZATION_ADMIN_OPTION)) {
                        query.append(NoteDBAdaptor.QueryParams.VISIBILITY.key(), Note.Visibility.PUBLIC.name());
                    }

                    QueryOptions noteOptions = createInnerQueryOptionsForVersionedEntity(options,
                            OrganizationDBAdaptor.QueryParams.NOTES.key(), true);
                    noteOptions.put(QueryOptions.LIMIT, 1000);
                    try {
                        OpenCGAResult<Document> result = noteMongoDBAdaptor.nativeGet(clientSession, query, noteOptions);
                        organizationDocument.put(OrganizationDBAdaptor.QueryParams.NOTES.key(), result.getResults());
                    } catch (CatalogDBException e) {
                        logger.warn("Could not obtain the organization notes", e);
                    }
                }

                List<String> projectField = Collections.singletonList(OrganizationDBAdaptor.QueryParams.PROJECTS.key());
                if (includeField(options, projectField)) {
                    OpenCGAResult<Document> openCGAResult = null;
                    try {
                        if (StringUtils.isNotEmpty(user)) {
                            openCGAResult = projectMongoDBAdaptor.nativeGet(clientSession, new Query(), projectOptions, user);
                        } else {
                            openCGAResult = projectMongoDBAdaptor.nativeGet(clientSession, new Query(), projectOptions);
                        }
                    } catch (CatalogDBException | CatalogAuthorizationException e) {
                        logger.warn("Could not fetch projects for organization.", e);
                    }
                    organizationDocument.put(OrganizationDBAdaptor.QueryParams.PROJECTS.key(), openCGAResult.getResults());
                }
            }

            organizationListBuffer.add(organizationDocument);
        }
    }
}
