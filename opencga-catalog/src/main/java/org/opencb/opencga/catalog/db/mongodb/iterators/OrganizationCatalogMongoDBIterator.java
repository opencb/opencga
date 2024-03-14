package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
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
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.models.notes.Note;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class OrganizationCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    private final QueryOptions options;

    private final Queue<Document> organizationListBuffer;
    private final NoteMongoDBAdaptor noteMongoDBAdaptor;
    private MigrationMongoDBAdaptor migrationDBAdaptor;

    private final Logger logger;


    public OrganizationCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                         GenericDocumentComplexConverter<E> converter, OrganizationMongoDBAdaptorFactory dbAdaptorFactory,
                                         QueryOptions options) {
        super(mongoCursor, clientSession, converter, null);

        this.options = options != null ? new QueryOptions(options) : new QueryOptions();

        this.migrationDBAdaptor = dbAdaptorFactory.getMigrationDBAdaptor();
        this.noteMongoDBAdaptor = dbAdaptorFactory.getCatalogNotesDBAdaptor();

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
                if (includeField(migrationFields)) {
                    List<Document> migrationRuns = migrationDBAdaptor.nativeGet().getResults();
                    Document internal = organizationDocument.get(OrganizationDBAdaptor.QueryParams.INTERNAL.key(), Document.class);
                    if (internal == null) {
                        internal = new Document();
                        organizationDocument.put(OrganizationDBAdaptor.QueryParams.INTERNAL.key(), internal);
                    }
                    internal.put("migrationExecutions", migrationRuns);
                }

                List<String> noteField = Collections.singletonList(OrganizationDBAdaptor.QueryParams.NOTES.key());
                if (includeField(noteField)) {
                    Query query = new Query(NoteDBAdaptor.QueryParams.SCOPE.key(), Note.Scope.ORGANIZATION.name());
                    if (!options.getBoolean(OrganizationDBAdaptor.IS_ORGANIZATION_ADMIN_OPTION)) {
                        query.append(NoteDBAdaptor.QueryParams.VISIBILITY.key(), Note.Visibility.PUBLIC.name());
                    }

                    QueryOptions noteOptions = createInnerQueryOptionsForVersionedEntity(options,
                            OrganizationDBAdaptor.QueryParams.NOTES.key(), true);
                    try {
                        OpenCGAResult<Document> result = noteMongoDBAdaptor.nativeGet(clientSession, query, noteOptions);
                        organizationDocument.put(OrganizationDBAdaptor.QueryParams.NOTES.key(), result.getResults());
                    } catch (CatalogDBException e) {
                        logger.warn("Could not obtain the organization notes", e);
                    }
                }
            }

            organizationListBuffer.add(organizationDocument);
        }
    }

    private boolean includeField(List<String> fields) {
        Set<String> includedFields = new HashSet<>(fields);
        if (options.containsKey(QueryOptions.INCLUDE)) {
            List<String> currentIncludeList = options.getAsStringList(QueryOptions.INCLUDE);
            for (String include : currentIncludeList) {
                if (includedFields.contains(include)) {
                    return true;
                }
            }
            return false;
        } else if (options.containsKey(QueryOptions.EXCLUDE)) {
            List<String> currentExcludeList = options.getAsStringList(QueryOptions.EXCLUDE);
            for (String exclude : currentExcludeList) {
                if (includedFields.contains(exclude)) {
                    return false;
                }
            }
            return true;
        }

        return true;
    }
}
