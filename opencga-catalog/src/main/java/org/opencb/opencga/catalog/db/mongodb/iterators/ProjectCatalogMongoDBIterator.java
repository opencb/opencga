package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.StudyMongoDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ProjectCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    private final String user;

    private final QueryOptions options;
    private final StudyMongoDBAdaptor studyDBAdaptor;
    private QueryOptions studyQueryOptions;

    private final Queue<Document> projectListBuffer;

    private final Logger logger;

    private static final int BUFFER_SIZE = 100;

    private static final String UID = ProjectDBAdaptor.QueryParams.UID.key();

    public ProjectCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                         GenericDocumentComplexConverter<E> converter, MongoDBAdaptorFactory dbAdaptorFactory,
                                         QueryOptions options, String user) {
        super(mongoCursor, clientSession, converter, null);

        this.studyDBAdaptor = dbAdaptorFactory.getCatalogStudyDBAdaptor();

        this.options = options != null ? new QueryOptions(options) : new QueryOptions();
        this.studyQueryOptions = createInnerQueryOptionsForVersionedEntity(this.options, ProjectDBAdaptor.QueryParams.STUDIES.key(), false);
        this.studyQueryOptions = MongoDBAdaptor.filterQueryOptions(this.studyQueryOptions,
                Collections.singletonList(MongoDBAdaptor.PRIVATE_PROJECT));

        this.user = user;

        this.projectListBuffer = new LinkedList<>();
        this.logger = LoggerFactory.getLogger(ProjectCatalogMongoDBIterator.class);
    }

    @Override
    public boolean hasNext() {
        if (projectListBuffer.isEmpty()) {
            fetchNextBatch();
        }
        return !projectListBuffer.isEmpty();
    }

    @Override
    public E next() {
        Document next = projectListBuffer.remove();

        if (filter != null) {
            next = filter.apply(next);
        }

//        addAclInformation(next, options);

        if (converter != null) {
            return (E) converter.convertToDataModelType(next);
        } else {
            return (E) next;
        }
    }

    private void fetchNextBatch() {
        Set<Long> projectUidSet = new HashSet<>();

        // Get next BUFFER_SIZE documents
        int counter = 0;
        while (mongoCursor.hasNext() && counter < BUFFER_SIZE) {
            Document projectDocument = mongoCursor.next().get("projects", Document.class);

            projectListBuffer.add(projectDocument);
            counter++;

            if (options == null || !options.containsKey(QueryOptions.EXCLUDE)
                    || !options.getAsStringList(QueryOptions.EXCLUDE).contains("projects.studies")) {
                projectUidSet.add(projectDocument.get(UID, Long.class));
            }
        }

        if (!projectUidSet.isEmpty()) {
            OpenCGAResult<Document> studyResult;
            Query studyQuery = new Query(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), new ArrayList<>(projectUidSet));
            try {
                if (StringUtils.isNotEmpty(user)) {
                    studyResult = studyDBAdaptor.nativeGet(clientSession, studyQuery, studyQueryOptions, user);
                } else {
                    studyResult = studyDBAdaptor.nativeGet(clientSession, studyQuery, studyQueryOptions);
                }
            } catch (CatalogDBException | CatalogAuthorizationException e) {
                logger.warn("Could not obtain the studies associated to the projects: {}", e.getMessage(), e);
                return;
            }

            Map<Long, List<Document>> projectStudiesMap = new HashMap<>();
            for (Document studyDoc : studyResult.getResults()) {
                Document project = studyDoc.get(MongoDBAdaptor.PRIVATE_PROJECT, Document.class);
                long projectUid = project.get(UID, Long.class);
                if (!projectStudiesMap.containsKey(projectUid)) {
                    projectStudiesMap.put(projectUid, new LinkedList<>());
                }
                projectStudiesMap.get(projectUid).add(studyDoc);
            }

            if (!projectStudiesMap.isEmpty()) {
                projectListBuffer.forEach(project -> {
                    long projectUid = project.get(UID, Long.class);
                    if (projectStudiesMap.containsKey(projectUid)) {
                        project.put(ProjectDBAdaptor.QueryParams.STUDIES.key(), projectStudiesMap.get(projectUid));
                    }
                });
            }
        }
    }
}
