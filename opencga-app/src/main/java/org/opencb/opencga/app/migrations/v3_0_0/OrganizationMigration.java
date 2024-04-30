package org.opencb.opencga.app.migrations.v3_0_0;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.opencga.catalog.auth.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.migration.MigrationRun;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

@Migration(id = "add_organizations", description = "Add new Organization layer #TASK-4389", version = "3.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20231212)
public class OrganizationMigration extends MigrationTool {
    private final Configuration configuration;
    private final String adminPassword;
    private String userId;

    private MongoDBAdaptorFactory mongoDBAdaptorFactory;
    private String oldDatabase;
    private MongoDataStore oldDatastore;
    private Set<String> userIdsToDiscardData;

    private MigrationStatus status;

    private enum MigrationStatus {
        MIGRATED,
        PENDING_MIGRATION,
        ERROR
    }

    public OrganizationMigration(Configuration configuration, String adminPassword, String userId) throws CatalogException {
        this.configuration = configuration;
        this.adminPassword = adminPassword;
        this.userId = userId;

        this.status = checkAndInit();
    }

    private MigrationStatus checkAndInit() throws CatalogException {
        this.oldDatabase = configuration.getDatabasePrefix() + "_catalog";
        this.mongoDBAdaptorFactory = new MongoDBAdaptorFactory(configuration, new IOManagerFactory());
        this.oldDatastore = mongoDBAdaptorFactory.getMongoManager().get(oldDatabase, mongoDBAdaptorFactory.getMongoDbConfiguration());

        MongoCollection<Document> userCol = oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.USER_COLLECTION);
        FindIterable<Document> iterable = userCol.find(Filters.eq("id", ParamConstants.OPENCGA_USER_ID));
        try (MongoCursor<Document> cursor = iterable.cursor()) {
            if (!cursor.hasNext()) {
                MongoIterable<String> collectionNames = oldDatastore.getDb().listCollectionNames();
                try (MongoCursor<String> tmpCursor = collectionNames.cursor()) {
                    if (!tmpCursor.hasNext()) {
                        logger.info("Database '{}' not found. Database already migrated.", this.oldDatabase);
                        return MigrationStatus.MIGRATED;
                    } else {
                        List<String> collections = new LinkedList<>();
                        tmpCursor.forEachRemaining(collections::add);
                        logger.debug("Found '{}' collections in '{}' database", StringUtils.join(collections, ", "), this.oldDatabase);
                        return MigrationStatus.ERROR;
                    }
                }
            }
            Document userDoc = cursor.next();
            String password = userDoc.getString("_password");
            // Check admin password
            try {
                if (!CryptoUtils.sha1(adminPassword).equals(password)) {
                    throw CatalogAuthorizationException.opencgaAdminOnlySupportedOperation();
                }
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        // Check write access to file system
        if (!Files.isWritable(Paths.get(configuration.getWorkspace()))) {
            throw new CatalogException("Please grant write access to path '" + configuration.getWorkspace() + "'");
        }

        // TODO: Check last migration is 2.2.??
        MongoCollection<Document> migrationCol = oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.MIGRATION_COLLECTION);
        long count = migrationCol.countDocuments(Filters.in("status", MigrationRun.MigrationStatus.PENDING,
                MigrationRun.MigrationStatus.ON_HOLD, MigrationRun.MigrationStatus.ERROR));
        if (count > 0) {
            throw new CatalogException("Please check past migrations. Found migrations with status '" + MigrationRun.MigrationStatus.PENDING
                    + "', '" + MigrationRun.MigrationStatus.ON_HOLD + "', or '" + MigrationRun.MigrationStatus.ERROR + "'.");
        }

        // Retrieve all users with data
        DistinctIterable<String> userIdIterable = userCol.distinct("id",
                Filters.and(
                        Filters.ne("projects", null),
                        Filters.ne("projects", Collections.emptyList())),
                String.class);
        List<String> userIds = new LinkedList<>();
        try (MongoCursor<String> iterator = userIdIterable.iterator()) {
            while (iterator.hasNext()) {
                String tmpUserId = iterator.next();
                if (!ParamConstants.OPENCGA_USER_ID.equals(tmpUserId)) {
                    userIds.add(tmpUserId);
                }
            }
        }
        if (StringUtils.isNotEmpty(userId)) {
            if (!userIds.contains(userId)) {
                throw new CatalogException("User '" + userId + "' does not have any data. Available users to migrate are "
                        + userIds.stream()
                        .collect(Collectors.joining(", ")));
            }
            // Extract users that will need to remove data from
            this.userIdsToDiscardData = userIds.stream()
                    .filter(u -> !userId.equals(u))
                    .collect(Collectors.toSet());
        } else {
            if (userIds.size() > 1) {
                throw new CatalogException("More than 1 user containing data found. Available users to migrate are "
                        + StringUtils.join(userIds, ", ") + ". Please, choose which one to migrate.");
            } else if (userIds.isEmpty()) {
                throw new CatalogException("No users found to migrate.");
            }
            this.userId = userIds.get(0);
            this.userIdsToDiscardData = new HashSet<>();
        }

        this.organizationId = this.userId;
        this.catalogManager = new CatalogManager(configuration);
        return MigrationStatus.PENDING_MIGRATION;
    }

    @Override
    protected void run() throws Exception {
        if (this.status == MigrationStatus.ERROR) {
            throw new CatalogException("Corrupted database '" + this.oldDatabase + "' found. Could not migrate.");
        } else if (this.status == MigrationStatus.MIGRATED) {
            return;
        }

        MongoCollection<Document> metadataCol = oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.METADATA_COLLECTION);
        FindIterable<Document>  iterable = metadataCol.find(new Document());
        Document metaDoc;
        long counter;
        String secretKey;
        String algorithm;
        try (MongoCursor<Document> cursor = iterable.cursor()) {
            metaDoc = cursor.next();
            counter = metaDoc.get("idCounter", Number.class).longValue();
            Document admin = metaDoc.get("admin", Document.class);
            secretKey = admin.getString("secretKey");
            algorithm = admin.getString("algorithm");
        }

        if (!userIdsToDiscardData.isEmpty()) {
            MongoCollection<Document> studyCol = oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION);
            // First, remove unnecessary data
            queryMongo(studyCol, Filters.in("_ownerId", userIdsToDiscardData), Projections.include("uid"), studyDoc -> {
                long studyUid = studyDoc.get("uid", Number.class).longValue();
                Bson query = Filters.eq("studyUid", studyUid);

                // Delete data associated to the study
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.FILE_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.SAMPLE_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.INDIVIDUAL_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.COHORT_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.FAMILY_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.PANEL_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.INTERPRETATION_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.FAMILY_ARCHIVE_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.PANEL_ARCHIVE_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.DELETED_FILE_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.DELETED_JOB_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.DELETED_SAMPLE_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.DELETED_INDIVIDUAL_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.DELETED_COHORT_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.DELETED_FAMILY_COLLECTION).deleteMany(query);
                oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.DELETED_PANEL_COLLECTION).deleteMany(query);
            });

            // Delete studies
            DeleteResult result = studyCol.deleteMany(Filters.in("_ownerId", userIdsToDiscardData));
            if (result.getDeletedCount() > 0) {
                logger.info("Deleted {} unnecessary studies", result.getDeletedCount());
            }

            // Remove projects from users
            oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.USER_COLLECTION).updateMany(
                    Filters.in("id", userIdsToDiscardData),
                    Updates.set("projects", Collections.emptyList())
            );
        }

        // Create admin organization
        catalogManager.installCatalogDB(algorithm, secretKey, "MyStr0NgT!mP0r4lP4sWSoRd", "tempmail@tempmail.com", false);

        // Create admin organization
        MongoCollection<Document> oldUserCollection = oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.USER_COLLECTION);
        queryMongo(oldUserCollection, Filters.eq("id", ParamConstants.OPENCGA_USER_ID), Projections.exclude("_id"), document -> {
            String organizationId = ParamConstants.ADMIN_ORGANIZATION;
            try {
                MongoCollection<Document> userCollection = mongoDBAdaptorFactory.getMongoDataStore(organizationId).getDb()
                        .getCollection(OrganizationMongoDBAdaptorFactory.USER_COLLECTION);
                MongoCollection<Document> projectCollection = mongoDBAdaptorFactory.getMongoDataStore(organizationId).getDb()
                        .getCollection(OrganizationMongoDBAdaptorFactory.PROJECT_COLLECTION);
                MongoCollection<Document> studyCollection = mongoDBAdaptorFactory.getMongoDataStore(organizationId).getDb()
                        .getCollection(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION);
                MongoCollection<Document> fileCollection = mongoDBAdaptorFactory.getMongoDataStore(organizationId).getDb()
                        .getCollection(OrganizationMongoDBAdaptorFactory.FILE_COLLECTION);
                // Empty data from collections with default data
                userCollection.deleteMany(new Document());
                projectCollection.deleteMany(new Document());
                studyCollection.deleteMany(new Document());
                fileCollection.deleteMany(new Document());

                // Replace user in organization
                userCollection.insertOne(document);

                MongoDatabase orgDatabase = mongoDBAdaptorFactory.getMongoDataStore(organizationId).getDb();

                // Extract projects
                List<Document> projects = document.getList("projects", Document.class);
                for (Document project : projects) {
                    long projectUid = project.get("uid", Number.class).longValue();

                    // Look for studies
                    MongoCollection<Document> oldStudyCol = oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION);
                    MongoCollection<Document> orgStudyCol = orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION);

                    queryMongo(oldStudyCol, Filters.eq("_project.uid", projectUid), Projections.exclude("_id"), studyDoc -> {
                        // Insert study in new organization collection
                        orgStudyCol.insertOne(studyDoc);

                        long studyUid = studyDoc.get("uid", Number.class).longValue();

                        // Move data belonging to the study
                        replicateData(studyUid, oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.SAMPLE_COLLECTION),
                                orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.SAMPLE_COLLECTION));
                        replicateData(studyUid, oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.INDIVIDUAL_COLLECTION),
                                orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.INDIVIDUAL_COLLECTION));
                        replicateData(studyUid, oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.COHORT_COLLECTION),
                                orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.COHORT_COLLECTION));
                        replicateData(studyUid, oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.FILE_COLLECTION),
                                orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.FILE_COLLECTION));
                        replicateData(studyUid, oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.FAMILY_COLLECTION),
                                orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.FAMILY_COLLECTION));
                        replicateData(studyUid, oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.PANEL_COLLECTION),
                                orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.PANEL_COLLECTION));
                        replicateData(studyUid, oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION),
                                orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION));
                        replicateData(studyUid, oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION),
                                orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION));
                        replicateData(studyUid, oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.INTERPRETATION_COLLECTION),
                                orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.INTERPRETATION_COLLECTION));


                        oldStudyCol.deleteOne(Filters.eq("uid", studyUid));
                    });

                }

                // Remove user from the source database
                oldUserCollection.deleteOne(Filters.eq("id", ParamConstants.OPENCGA_USER_ID));
            } catch (CatalogException e) {
                throw new RuntimeException(e);
            }
        });

        // Create new organization
        String opencgaToken = catalogManager.getUserManager().login(ParamConstants.ADMIN_ORGANIZATION, ParamConstants.OPENCGA_USER_ID, adminPassword).getToken();
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(organizationId), null, opencgaToken);
        OrganizationMongoDBAdaptorFactory orgFactory = mongoDBAdaptorFactory.getOrganizationMongoDBAdaptorFactory(organizationId);
        String newDatabase = orgFactory.getMongoDataStore().getDatabaseName();

        // Rename database to main organization database
        MongoDataStore adminDatastore = mongoDBAdaptorFactory.getMongoManager().get("admin", mongoDBAdaptorFactory.getMongoDbConfiguration());
        for (String collectionName : oldDatastore.getCollectionNames()) {
            if (!OrganizationMongoDBAdaptorFactory.MIGRATION_COLLECTION.equals(collectionName)) {
                logger.info("Renaming collection {} to {}", oldDatabase + "." + collectionName, newDatabase + "." + collectionName);
                adminDatastore.getDb().runCommand(new Document()
                        .append("renameCollection", oldDatabase + "." + collectionName)
                        .append("to", newDatabase + "." + collectionName)
                        .append("dropTarget", true)
                );
            } else {
                // Remove collections
                oldDatastore.getDb().getCollection(collectionName).drop();
            }
        }

        CatalogIOManager ioManager = new CatalogIOManager(configuration);

        // Loop over all organizations to perform additional data model changes
        for (String organizationId : mongoDBAdaptorFactory.getOrganizationIds()) {
            ioManager.createOrganization(organizationId);

            MongoDatabase database = mongoDBAdaptorFactory.getMongoDataStore(organizationId).getDb();
            MongoCollection<Document> userCollection = database.getCollection(OrganizationMongoDBAdaptorFactory.USER_COLLECTION);

            // Extract projects from users
            queryMongo(userCollection, new Document(), Projections.exclude("_id"), document -> {
                List<Document> projects = document.getList("projects", Document.class);
                if (CollectionUtils.isNotEmpty(projects)) {
                    // Create project directory
                    for (Document project : projects) {
                        Long projectUid = project.get("uid", Long.class);
                        try {
                            ioManager.createProject(organizationId, Long.toString(projectUid));
                        } catch (CatalogIOException e) {
                            throw new RuntimeException("Couldn't create project folder for project '" + project.getString("fqn") + "'.", e);
                        }
                    }

                    MongoCollection<Document> projectCol = database.getCollection(OrganizationMongoDBAdaptorFactory.PROJECT_COLLECTION);
                    projectCol.insertMany(projects);
                }
            });

            // Remove account type, projects and sharedProjects from User
            for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.USER_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_USER_COLLECTION)) {
                MongoCollection<Document> mongoCollection = database.getCollection(collection);
                mongoCollection.updateMany(new Document(),
                        Updates.combine(
                                Updates.set("projects", Collections.emptyList()),
                                Updates.set("organization", organizationId),
                                Updates.unset("sharedProjects"),
                                Updates.unset("account.type")
                        )
                );
            }

            // Add owner as admin of every study and remove _ownerId field
            for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION)) {
                MongoCollection<Document> mongoCollection = database.getCollection(collection);
                mongoCollection.updateMany(
                        Filters.eq(StudyDBAdaptor.QueryParams.GROUP_ID.key(), ParamConstants.ADMINS_GROUP),
                        Updates.combine(
                                Updates.unset("_ownerId"),
                                Updates.push("groups.$.userIds", organizationId)
                ));
            }

            // Set organization counter
            MongoCollection<Document> orgCol = database.getCollection(OrganizationMongoDBAdaptorFactory.ORGANIZATION_COLLECTION);
            
            List<Document> authOrigins = new ArrayList<>();
            // Super admins organization will only have one authentication origin - internal
            if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationId)) {
                if (configuration.getAuthentication() != null && CollectionUtils.isNotEmpty(configuration.getAuthentication().getAuthenticationOrigins())) {
                    for (AuthenticationOrigin authenticationOrigin : configuration.getAuthentication().getAuthenticationOrigins()) {
                        if (authenticationOrigin.getType().equals(AuthenticationOrigin.AuthenticationType.OPENCGA)
                                && "internal".equals(authenticationOrigin.getId())) {
                            continue;
                        }
                        Document authOriginDoc = convertToDocument(authenticationOrigin);
                        authOriginDoc.put("algorithm", algorithm);
                        authOriginDoc.put("secretKey", secretKey);
                        authOriginDoc.put("expiration", 3600L);
                        authOrigins.add(authOriginDoc);
                    }
                }
            }
            Document authOriginDoc = convertToDocument(CatalogAuthenticationManager.createOpencgaAuthenticationOrigin());
            authOriginDoc.put("id", "internal");
            authOriginDoc.put("algorithm", algorithm);
            authOriginDoc.put("secretKey", secretKey);
            authOriginDoc.put("expiration", 3600L);

            // Set organization counter, owner and authOrigins
            orgCol.updateOne(Filters.eq("id", organizationId), Updates.combine(
                    Updates.set("_idCounter", counter),
                    Updates.set("owner", organizationId),
                    Updates.set("configuration.authenticationOrigins", authOrigins)
            ));
        }
    }

    private void replicateData(long studyUid, MongoCollection<Document> sourceCol, MongoCollection<Document> targetCol) {
        // Move data to the new collection
        logger.info("Moving data from {} to {}", sourceCol.getNamespace(), targetCol.getNamespace());
        migrateCollection(sourceCol, targetCol, Filters.eq("studyUid", studyUid), Projections.exclude("_id"),
                (document, bulk) -> bulk.add(new InsertOneModel<>(document)));
        // Remove data from the source collection
        sourceCol.deleteMany(Filters.eq("studyUid", studyUid));
    }

}
