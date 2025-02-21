package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
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
    private boolean includeStudyInfo;
    private final OrganizationMongoDBAdaptor organizationMongoDBAdaptor;
    private final StudyMongoDBAdaptor studyDBAdaptor;
    private QueryOptions studyQueryOptions;

    private final Queue<Document> projectListBuffer;
    private List<Document> federationClients;

    private final Logger logger;

    private static final int BUFFER_SIZE = 100;

    private static final String UID = ProjectDBAdaptor.QueryParams.UID.key();

    public ProjectCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                         GenericDocumentComplexConverter<E> converter, OrganizationMongoDBAdaptorFactory dbAdaptorFactory,
                                         QueryOptions options, String user) {
        super(mongoCursor, clientSession, converter, null);

        this.organizationMongoDBAdaptor = dbAdaptorFactory.getCatalogOrganizationDBAdaptor();
        this.studyDBAdaptor = dbAdaptorFactory.getCatalogStudyDBAdaptor();

        this.options = options != null ? new QueryOptions(options) : new QueryOptions();
        this.includeStudyInfo = includeStudyInfo();
        this.studyQueryOptions = createInnerQueryOptionsForVersionedEntity(this.options, ProjectDBAdaptor.QueryParams.STUDIES.key(), false);
        this.studyQueryOptions = MongoDBAdaptor.filterQueryOptionsToIncludeKeys(this.studyQueryOptions,
                Collections.singletonList(MongoDBAdaptor.PRIVATE_PROJECT));

        this.user = user;

        this.federationClients = null;
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
            Document projectDocument = mongoCursor.next();

            projectListBuffer.add(projectDocument);
            counter++;

            if (includeStudyInfo) {
                projectUidSet.add(projectDocument.get(UID, Long.class));
            }
        }

        addFederationRef();
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

    private List<Document> getFederationClients() {
        if (federationClients == null) {
            // The study is federated. We need to fetch the information from the corresponding collection
            QueryOptions orgOptions = new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.FEDERATION_CLIENTS.key());
            try {
                Document organization = organizationMongoDBAdaptor.nativeGet(clientSession, user, orgOptions).first();
                Document orgFederation = organization.get(OrganizationDBAdaptor.QueryParams.FEDERATION.key(), Document.class);
                if (orgFederation == null) {
                    logger.warn("Federation information could not be filled in. Organization was not found.");
                    // Remove null so we don't try to fetch the information again
                    federationClients = Collections.emptyList();
                    return federationClients;
                }
                federationClients = orgFederation.getList("clients", Document.class);
                if (federationClients == null) {
                    logger.warn("Federation information could not be filled in. Federation clients were not found.");
                    // Remove null so we don't try to fetch the information again
                    federationClients = Collections.emptyList();
                }
            } catch (CatalogDBException e) {
                logger.warn("Could not obtain the Organization information", e);
            }
        }
        return federationClients;
    }

    private void addFederationRef() {
        projectListBuffer.forEach(project -> {
            Document federation = project.get(ProjectDBAdaptor.QueryParams.FEDERATION.key(), Document.class);
            if (federation == null) {
                return;
            }
            String federationId = federation.getString("id");
            if (StringUtils.isEmpty(federationId)) {
                return;
            }
            List<Document> federationClients = getFederationClients();
            for (Document client : federationClients) {
                String clientId = client.getString("id");
                if (federationId.equals(clientId)) {
                    project.put(ProjectDBAdaptor.QueryParams.FEDERATION.key(), client);
                    return;
                }
            }
        });
    }

    private boolean includeStudyInfo() {
        if (options == null) {
            return true;
        }
        if (options.containsKey(QueryOptions.EXCLUDE)) {
            List<String> list = options.getAsStringList(QueryOptions.EXCLUDE);
            for (String exclude : list) {
                if (exclude.equals("studies") || exclude.equals("projects.studies")) {
                    return false;
                }
            }
            return true;
        } else if (options.containsKey(QueryOptions.INCLUDE)) {
            List<String> list = options.getAsStringList(QueryOptions.INCLUDE);
            for (String include : list) {
                if (include.startsWith("studies") || include.startsWith("projects.studies")) {
                    return true;
                }
            }
            return false;
        } else {
            return true;
        }
    }
}
