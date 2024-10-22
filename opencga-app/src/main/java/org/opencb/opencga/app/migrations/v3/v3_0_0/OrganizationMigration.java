package org.opencb.opencga.app.migrations.v3.v3_0_0;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
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
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.catalog.utils.FqnUtils;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.migration.MigrationRun;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Modifier;
import java.net.URL;
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

    public OrganizationMigration(Configuration configuration, String adminPassword, String userId, String organizationId)
            throws CatalogException {
        this.configuration = configuration;
        this.adminPassword = adminPassword;
        this.userId = userId;
        this.organizationId = organizationId;

        this.status = checkAndInit();
    }

    private MigrationStatus checkAndInit() throws CatalogException {
        this.oldDatabase = configuration.getDatabasePrefix() + "_catalog";
        this.mongoDBAdaptorFactory = new MongoDBAdaptorFactory(configuration);
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

        Set<Class<? extends MigrationTool>> availableMigrations = getAvailableMigrations();

        // Check all previous v2.x migrations have been run successfully
        MongoCollection<Document> migrationCol = oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.MIGRATION_COLLECTION);
        long count = migrationCol.countDocuments();
        // We take out 1 to the availableMigrations.size() because it will consider the present migration, which is not part of the check
        logger.debug("Found '{}' Java migrations", availableMigrations.size() - 1);
        logger.debug("Found '{}' migrations registered in the database", count);
        if (count < availableMigrations.size() - 1) {
            Set<String> migrations = new HashSet<>();
            for (Class<? extends MigrationTool> availableMigration : availableMigrations) {
                Migration annotation = availableMigration.getAnnotation(Migration.class);
                // Only consider previous migrations, not the present one
                if (!annotation.version().equals("3.0.0")) {
                    migrations.add(annotation.id() + ":v" + annotation.version());
                }
            }

            Set<String> processedMigrations = new HashSet<>();
            Set<String> onlyInJava = new HashSet<>();
            Set<String> onlyInDB = new HashSet<>();
            migrationCol.find().forEach((document) -> {
                String migrationId = document.getString("id") + ":v" + document.getString("version");
                processedMigrations.add(migrationId);
                if (!migrations.contains(migrationId)) {
                    onlyInDB.add(migrationId);
                }
            });
            for (String migration : migrations) {
                if (!processedMigrations.contains(migration)) {
                    onlyInJava.add(migration);
                }
            }
            logger.debug("Migrations not registered in the database: {}", String.join(", ", onlyInJava));
            logger.debug("Migrations only found in the DB: {}", String.join(", ", onlyInDB));

            throw new CatalogException("Please check past migrations before moving to v3.0.0. Found "
                    + (availableMigrations.size() -1 - count) + " missing migrations available.");
        }

        count = migrationCol.countDocuments(Filters.in("status", MigrationRun.MigrationStatus.PENDING,
                MigrationRun.MigrationStatus.ON_HOLD, MigrationRun.MigrationStatus.ERROR));
        if (count > 0) {
            throw new CatalogException("Please check past migrations. Found " + count + " migrations with status '"
                    + MigrationRun.MigrationStatus.PENDING + "', '" + MigrationRun.MigrationStatus.ON_HOLD + "', or '"
                    + MigrationRun.MigrationStatus.ERROR + "'.");
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

        if (StringUtils.isEmpty(this.organizationId)) {
            this.organizationId = this.userId;
        }
        ParamUtils.checkIdentifier(this.organizationId, "Organization id");
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
                MongoDatabase orgDatabase = mongoDBAdaptorFactory.getMongoDataStore(organizationId).getDb();

                MongoCollection<Document> userCollection = orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.USER_COLLECTION);
                MongoCollection<Document> projectCollection = orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.PROJECT_COLLECTION);
                MongoCollection<Document> studyCollection = orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION);
                MongoCollection<Document> fileCollection = orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.FILE_COLLECTION);
                MongoCollection<Document> migrationCollection = orgDatabase.getCollection(OrganizationMongoDBAdaptorFactory.MIGRATION_COLLECTION);
                // Empty data from collections with default data
                userCollection.deleteMany(new Document());
                projectCollection.deleteMany(new Document());
                studyCollection.deleteMany(new Document());
                fileCollection.deleteMany(new Document());
                migrationCollection.deleteMany(new Document());

                // Replace user in organization
                userCollection.insertOne(document);

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

                // Copy migration history
                MongoCollection<Document> oldMigrationCollection = oldDatastore.getDb().getCollection(OrganizationMongoDBAdaptorFactory.MIGRATION_COLLECTION);
                logger.info("Copying Migration data from {} to {}", oldMigrationCollection.getNamespace(), migrationCollection.getNamespace());
                migrateCollection(oldMigrationCollection, migrationCollection, new Document(), Projections.exclude("_id"),
                        (tmpDocument, bulk) -> bulk.add(new InsertOneModel<>(tmpDocument)));

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
            logger.info("Renaming collection {} to {}", oldDatabase + "." + collectionName, newDatabase + "." + collectionName);
            adminDatastore.getDb().runCommand(new Document()
                    .append("renameCollection", oldDatabase + "." + collectionName)
                    .append("to", newDatabase + "." + collectionName)
                    .append("dropTarget", true)
            );
        }

        CatalogIOManager ioManager = new CatalogIOManager(configuration);

        Map<String, String> organizationOwnerMap = new HashMap<>();
        organizationOwnerMap.put(ParamConstants.ADMIN_ORGANIZATION, ParamConstants.OPENCGA_USER_ID);
        organizationOwnerMap.put(this.organizationId, this.userId);

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
            String ownerId = organizationOwnerMap.get(organizationId);
            for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION)) {
                MongoCollection<Document> mongoCollection = database.getCollection(collection);
                mongoCollection.updateMany(
                        Filters.eq(StudyDBAdaptor.QueryParams.GROUP_ID.key(), ParamConstants.ADMINS_GROUP),
                        Updates.combine(
                                Updates.unset("_ownerId"),
                                Updates.push("groups.$.userIds", ownerId)
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
                                && authenticationOrigin.getId().equals(CatalogAuthenticationManager.INTERNAL)) {
                            continue;
                        }
                        authenticationOrigin.setAlgorithm(algorithm);
                        authenticationOrigin.setSecretKey(secretKey);
                        authenticationOrigin.setExpiration(3600);
                        authOrigins.add(convertToDocument(authenticationOrigin));
                    }
                }
            }
            authOrigins.add(convertToDocument(CatalogAuthenticationManager.createRandomInternalAuthenticationOrigin()));

            // Set organization counter, owner and authOrigins
            orgCol.updateOne(Filters.eq("id", organizationId), Updates.combine(
                    Updates.set("_idCounter", counter),
                    Updates.set("owner", ownerId),
                    Updates.set("configuration.authenticationOrigins", authOrigins)
            ));
        }

        // If the user didn't want to use the userId as the new organization id, we then need to change all the fqn's
        if (!this.organizationId.equals(this.userId)) {
            changeFqns();
        }

        // Skip current migration for both organizations
        catalogManager.getMigrationManager().skipPendingMigrations(ParamConstants.ADMIN_ORGANIZATION, opencgaToken);
        catalogManager.getMigrationManager().skipPendingMigrations(organizationId, opencgaToken);
    }

    private void changeFqns() throws CatalogDBException {
        this.dbAdaptorFactory = this.mongoDBAdaptorFactory;
        String date = TimeUtils.getTime();

        // Change project fqn's
        for (String projectCol : Arrays.asList(OrganizationMongoDBAdaptorFactory.PROJECT_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_PROJECT_COLLECTION)) {
            migrateCollection(projectCol, new Document(), Projections.include("_id", "id", "fqn"), (document, bulk) -> {
                String currentFqn = document.getString("fqn");
                String projectId = document.getString("id");
                String projectFqn = FqnUtils.buildFqn(this.organizationId, projectId);
                bulk.add(new UpdateOneModel<>(
                        Filters.eq("_id", document.get("_id")),
                        new Document("$set", new Document()
                                .append("fqn", projectFqn)
                                .append("attributes.OPENCGA.3_0_0", new Document()
                                        .append("date", date)
                                        .append("oldFqn", currentFqn)
                                )))
                );
            });
        }

        MongoDatabase database = mongoDBAdaptorFactory.getMongoDataStore(organizationId).getDb();
        MongoCollection<Document> jobCollection = database.getCollection(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION);
        MongoCollection<Document> jobDeletedCollection = database.getCollection(OrganizationMongoDBAdaptorFactory.DELETED_JOB_COLLECTION);

        // Change study fqn's
        for (String studyCol : Arrays.asList(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION)) {
            migrateCollection(studyCol, new Document(), Projections.include("_id", "uid", "fqn"), (document, bulk) -> {
                long studyUid = document.get("uid", Number.class).longValue();

                String oldStudyFqn = document.getString("fqn");
                FqnUtils.FQN oldFqnInstance = FqnUtils.parse(oldStudyFqn);
                String newFqn = FqnUtils.buildFqn(this.organizationId, oldFqnInstance.getProject(), oldFqnInstance.getStudy());
                bulk.add(new UpdateOneModel<>(
                        Filters.eq("_id", document.get("_id")),
                        new Document("$set", new Document()
                                .append("fqn", newFqn)
                                .append("attributes.OPENCGA.3_0_0", new Document()
                                        .append("date", date)
                                        .append("oldFqn", oldStudyFqn)
                                )
                        ))
                );

                // Change fqn in all jobs that were pointing to this study
                Bson jobQuery = Filters.eq("studyUid", studyUid);
                Bson update = new Document("$set", new Document()
                        .append("study.id", newFqn)
                        .append("attributes.OPENCGA.3_0_0", new Document()
                                .append("date", date)
                                .append("oldStudyFqn", oldStudyFqn)
                        )
                );
                jobCollection.updateMany(jobQuery, update);
                jobDeletedCollection.updateMany(jobQuery, update);
            });
        }
    }

    Set<Class<? extends MigrationTool>> getAvailableMigrations() {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(
                        new SubTypesScanner(),
                        new TypeAnnotationsScanner().filterResultsBy(s -> StringUtils.equals(s, Migration.class.getName()))
                )
                .addUrls(getUrls())
                .filterInputsBy(input -> input != null && input.endsWith(".class"))
        );

        Set<Class<? extends MigrationTool>> migrations = reflections.getSubTypesOf(MigrationTool.class);
        migrations.removeIf(c -> Modifier.isAbstract(c.getModifiers()));

        // Validate unique ids and rank
        Map<String, Set<String>> versionIdMap = new HashMap<>();

        for (Class<? extends MigrationTool> migration : migrations) {
            Migration annotation = migration.getAnnotation(Migration.class);

            if (!versionIdMap.containsKey(annotation.version())) {
                versionIdMap.put(annotation.version(), new HashSet<>());
            }
            if (versionIdMap.get(annotation.version()).contains(annotation.id())) {
                throw new IllegalStateException("Found duplicated migration id '" + annotation.id() + "' in version "
                        + annotation.version());
            }
            if (String.valueOf(annotation.date()).length() != 8) {
                throw new IllegalStateException("Found unexpected date '" + annotation.date() + "' in migration '" + annotation.id()
                        + "' from version " + annotation.version() + ". Date format is YYYYMMDD.");
            }
            versionIdMap.get(annotation.version()).add(annotation.id());
        }

        return migrations;
    }

    private Collection<URL> getUrls() {
        Collection<URL> urls = new LinkedList<>();
        for (URL url : ClasspathHelper.forPackage("org.opencb.opencga")) {
            String name = url.getPath().substring(url.getPath().lastIndexOf('/') + 1);
            if (name.isEmpty() || (name.contains("opencga") && !name.contains("opencga-hadoop-shaded"))) {
                urls.add(url);
            }
        }
        return urls;
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
