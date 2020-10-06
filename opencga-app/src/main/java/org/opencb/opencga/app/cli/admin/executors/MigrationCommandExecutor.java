package org.opencb.opencga.app.cli.admin.executors;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.opencga.app.cli.admin.executors.migration.AnnotationSetMigration;
import org.opencb.opencga.app.cli.admin.executors.migration.NewVariantMetadataMigration;
import org.opencb.opencga.app.cli.admin.executors.migration.storage.NewProjectMetadataMigration;
import org.opencb.opencga.app.cli.admin.executors.migration.storage.NewStudyMetadata;
import org.opencb.opencga.app.cli.admin.options.MigrationCommandOptions;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.CustomStatus;
import org.opencb.opencga.core.models.file.FileExperiment;
import org.opencb.opencga.core.models.file.FileInternal;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created on 08/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MigrationCommandExecutor extends AdminCommandExecutor {

    private final MigrationCommandOptions migrationCommandOptions;

    public MigrationCommandExecutor(MigrationCommandOptions migrationCommandOptions) {
        super(migrationCommandOptions.getCommonOptions());
        this.migrationCommandOptions = migrationCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing migration command line");

        String subCommandString = migrationCommandOptions.getSubCommand();
        switch (subCommandString) {
//            case "latest":
            case "v1.3.0":
                v1_3_0();
                break;
            case "v1.4.0":
                v1_4_0();
                break;
            case "v2.0.0":
                v2_0_0();
                break;
            default:
                logger.error("Subcommand '{}' not valid", subCommandString);
                break;
        }
    }

    private void v1_3_0() throws Exception {
        logger.info("MIGRATING v1.3.0");
        MigrationCommandOptions.MigrateV1_3_0CommandOptions options = migrationCommandOptions.getMigrateV130CommandOptions();

        if (options.files != null && !options.files.isEmpty()) {
            // Just migrate files. Do not even connect to Catalog!
            NewVariantMetadataMigration migration = new NewVariantMetadataMigration(storageConfiguration, null, options);
            for (String file : options.files) {
                migration.migrateVariantFileMetadataFile(Paths.get(file));
            }
        } else {
            setCatalogDatabaseCredentials(options, options.commonOptions);

            try (CatalogManager catalogManager = new CatalogManager(configuration)) {
                String sessionId = catalogManager.getUserManager().loginAsAdmin(options.commonOptions.adminPassword).getToken();

                // Catalog
                String basePath = appHome + "/misc/migration/v1.3.0/";

                String authentication = "";
                if (StringUtils.isNotEmpty(configuration.getCatalog().getDatabase().getUser())
                        && StringUtils.isNotEmpty(configuration.getCatalog().getDatabase().getPassword())) {
                    authentication = "-u " + configuration.getCatalog().getDatabase().getUser() + " -p "
                            + configuration.getCatalog().getDatabase().getPassword() + " --authenticationDatabase "
                            + configuration.getCatalog().getDatabase().getOptions().getOrDefault("authenticationDatabase", "admin") + " ";
                }

                String catalogCli = "mongo " + authentication + configuration.getCatalog().getDatabase().getHosts().get(0) + "/"
                        + catalogManager.getCatalogDatabase() + " opencga_catalog_v1.2.x_to_1.3.0.js";

                logger.info("Migrating Catalog. Running {} from {}", catalogCli, basePath);
                ProcessBuilder processBuilder = new ProcessBuilder(catalogCli.split(" "));
                processBuilder.directory(new File(basePath));
                Process p = processBuilder.start();

                BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String line;
                while ((line = input.readLine()) != null) {
                    logger.info(line);
                }
                input.close();

                p.waitFor();
                if (p.exitValue() != 0) {
                    throw new IllegalStateException("Error migrating catalog database!");
                }

                // Storage

                new NewVariantMetadataMigration(storageConfiguration, catalogManager, options).migrate(sessionId);
            }
        }
    }

    private void v1_4_0() throws Exception {
        MigrationCommandOptions.MigrateV1_4_0CommandOptions options = migrationCommandOptions.getMigrateV140CommandOptions();

        boolean skipAnnotations = false;
        boolean skipCatalogJS = false;
        boolean skipStorage = false;
        switch (options.what) {
            case CATALOG:
                skipStorage = true;
                break;
            case STORAGE:
                skipAnnotations = true;
                skipCatalogJS = true;
                break;
            case ANNOTATIONS:
                skipCatalogJS = true;
                skipStorage = true;
                break;
            case CATALOG_NO_ANNOTATIONS:
                skipAnnotations = true;
                skipStorage = true;
                break;
            case ALL:
            default:
                break;
        }

        setCatalogDatabaseCredentials(options, options.commonOptions);

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            // We get a non-expiring token
            String token = catalogManager.getUserManager().loginAsAdmin(options.commonOptions.adminPassword).getToken();
            String nonExpiringToken = catalogManager.getUserManager().getAdminNonExpiringToken(token);

            // Catalog
            if (!skipCatalogJS) {
                logger.info("Starting Catalog migration for 1.4.0");
                runMigration(catalogManager, appHome + "/misc/migration/v1.4.0/", "opencga_catalog_v1.3.x_to_1.4.0.js");
            }

            if (!skipAnnotations) {
                logger.info("Starting annotation migration for 1.4.0");

                // Migrate annotationSets
                new AnnotationSetMigration(catalogManager).migrate();

                logger.info("Finished annotation migration");
            }

            if (!skipStorage) {
                new NewProjectMetadataMigration(storageConfiguration, catalogManager, options).migrate(nonExpiringToken);
                new NewStudyMetadata(storageConfiguration, catalogManager).migrate(nonExpiringToken);
            }

        }
    }


    private void v2_0_0() throws Exception {
        MigrationCommandOptions.MigrateV2_0_0CommandOptions options = migrationCommandOptions.getMigrateV200CommandOptions();

        setCatalogDatabaseCredentials(options, options.commonOptions);

        boolean skipRc1 = false;
        boolean skipRc2 = false;
        switch (options.what) {
            case RC1:
                skipRc2 = true;
                break;
            case RC2:
                skipRc1 = true;
                break;
            case ALL:
            default:
                break;
        }

        // Check administrator password
        MongoDBAdaptorFactory factory = new MongoDBAdaptorFactory(configuration);
        MongoDBCollection metaCollection = factory.getMongoDBCollectionMap().get(MongoDBAdaptorFactory.METADATA_COLLECTION);

        if (!skipRc1) {
            String cypheredPassword = CryptoUtils.sha1(options.commonOptions.adminPassword);
            Document document = new Document("admin.password", cypheredPassword);
            if (metaCollection.count(document).getNumMatches() == 0) {
                throw CatalogAuthenticationException.incorrectUserOrPassword();
            }

            try (CatalogManager catalogManager = new CatalogManager(configuration)) {
                // 1. Catalog Javascript migration
                logger.info("Starting Catalog migration for 2.0.0 RC1");
                runMigration(catalogManager, appHome + "/misc/migration/v2.0.0-rc1/", "opencga_catalog_v1.4.2_to_v2.0.0-rc1.js");

                String token = catalogManager.getUserManager().loginAsAdmin(options.commonOptions.adminPassword).getToken();
                token = catalogManager.getUserManager().getAdminNonExpiringToken(token);

                // Create default project and study for administrator #1491
                catalogManager.getProjectManager().create("admin", "admin", "Default project", "", "", "", null, token);
                catalogManager.getStudyManager().create("admin", "admin", "admin", "admin", "Default study",
                        null, null, null, Collections.emptyMap(), null, token);

                // Create default JOBS folder for analysis
                MongoDBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(configuration);
                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.UID.key(),
                        StudyDBAdaptor.QueryParams.FQN.key(), StudyDBAdaptor.QueryParams.URI.key(), StudyDBAdaptor.QueryParams.RELEASE.key()));
                DBIterator<Study> iterator = dbAdaptorFactory.getCatalogStudyDBAdaptor().iterator(new Query(), queryOptions);

                Query fileQuery = new Query(FileDBAdaptor.QueryParams.PATH.key(), "JOBS/");
                while (iterator.hasNext()) {
                    Study study = iterator.next();
                    fileQuery.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
                    if (dbAdaptorFactory.getCatalogFileDBAdaptor().count(fileQuery).getNumMatches() == 0) {
                        logger.info("Creating JOBS/ folder for study {}", study.getFqn());

                        // JOBS folder does not exist
                        org.opencb.opencga.core.models.file.File file = new org.opencb.opencga.core.models.file.File("JOBS",
                                org.opencb.opencga.core.models.file.File.Type.DIRECTORY, org.opencb.opencga.core.models.file.File.Format.UNKNOWN,
                                org.opencb.opencga.core.models.file.File.Bioformat.UNKNOWN,
                                Paths.get(options.jobFolder).normalize().toAbsolutePath().resolve("JOBS").toUri(),
                                "JOBS/", null, TimeUtils.getTime(), TimeUtils.getTime(), "Default jobs folder",
                                false, 0, new Software(), new FileExperiment(), Collections.emptyList(), Collections.emptyList(), "",
                                study.getRelease(), Collections.emptyList(), Collections.emptyMap(), new CustomStatus(),
                                FileInternal.initialize(), Collections.emptyMap());
                        file.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.FILE));
                        file.setTags(Collections.emptyList());
                        file.setId(file.getPath().replace("/", ":"));

                        dbAdaptorFactory.getCatalogFileDBAdaptor().insert(study.getUid(), file, null, null, null, QueryOptions.empty());

                        // Create physical folder
                        catalogManager.getIoManagerFactory().get(file.getUri()).createDirectory(file.getUri(), true);
                    } else {
                        logger.info("JOBS/ folder already present for study {}", study.getFqn());
                    }
                }
            }
        }
        if (!skipRc2) {
            try (CatalogManager catalogManager = new CatalogManager(configuration)) {
                String adminToken = catalogManager.getUserManager().loginAsAdmin(options.commonOptions.adminPassword).getToken();
                adminToken = catalogManager.getUserManager().getAdminNonExpiringToken(adminToken);

                logger.info("Starting Catalog migration for 2.0.0 RC2");
                runMigration(catalogManager, appHome + "/misc/migration/v2.0.0-rc2/", "opencga_catalog_v2.0.0-rc1_to_v2.0.0-rc2.js");

                // Add automatically roles to all the family members
                QueryOptions familyUpdateOptions = new QueryOptions(ParamConstants.FAMILY_UPDATE_ROLES_PARAM, true);
                for (Project project : catalogManager.getProjectManager().get(new Query(), new QueryOptions(), adminToken).getResults()) {
                    if (project.getStudies() != null) {
                        for (Study study : project.getStudies()) {
                            logger.info("Updating family roles from study {}", study.getFqn());
                            catalogManager.getFamilyManager().update(study.getFqn(), new Query(), null, familyUpdateOptions, adminToken);
                        }
                    }
                }
            }
        }
    }

    private void runMigration(CatalogManager catalogManager, String scriptFolder, String scriptFileName)
            throws IOException, InterruptedException, CatalogException {
        String authentication = "";
        if (StringUtils.isNotEmpty(configuration.getCatalog().getDatabase().getUser())
                && StringUtils.isNotEmpty(configuration.getCatalog().getDatabase().getPassword())) {
            authentication = "-u " + configuration.getCatalog().getDatabase().getUser() + " -p "
                    + configuration.getCatalog().getDatabase().getPassword() + " --authenticationDatabase "
                    + configuration.getCatalog().getDatabase().getOptions().getOrDefault("authenticationDatabase", "admin") + " ";
        }
        if (configuration.getCatalog().getDatabase().getOptions() != null
                && configuration.getCatalog().getDatabase().getOptions().containsKey(MongoDBConfiguration.SSL_ENABLED)
                && Boolean.parseBoolean(configuration.getCatalog().getDatabase().getOptions().get(MongoDBConfiguration.SSL_ENABLED))) {
            authentication += "--ssl ";
        }
        if (configuration.getCatalog().getDatabase().getOptions() != null
                && configuration.getCatalog().getDatabase().getOptions().containsKey(MongoDBConfiguration.SSL_INVALID_CERTIFICATES_ALLOWED)
                && Boolean.parseBoolean(configuration.getCatalog().getDatabase().getOptions()
                .get(MongoDBConfiguration.SSL_INVALID_CERTIFICATES_ALLOWED))) {
            authentication += "--sslAllowInvalidCertificates ";
        }
        if (configuration.getCatalog().getDatabase().getOptions() != null
                && configuration.getCatalog().getDatabase().getOptions().containsKey(MongoDBConfiguration.SSL_INVALID_HOSTNAME_ALLOWED)
                && Boolean.parseBoolean(configuration.getCatalog().getDatabase().getOptions()
                .get(MongoDBConfiguration.SSL_INVALID_HOSTNAME_ALLOWED))) {
            authentication += "--sslAllowInvalidHostnames ";
        }

        String catalogCli = "mongo " + authentication
                + StringUtils.join(configuration.getCatalog().getDatabase().getHosts(), ",") + "/"
                + catalogManager.getCatalogDatabase() + " " + scriptFileName;

        logger.info("Migrating Catalog. Running {} from {}", catalogCli, scriptFolder);
        ProcessBuilder processBuilder = new ProcessBuilder(catalogCli.split(" "));
        processBuilder.directory(new File(scriptFolder));
        Process p = processBuilder.start();

        BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;
        while ((line = input.readLine()) != null) {
            logger.info(line);
        }
        p.waitFor();
        input.close();

        if (p.exitValue() == 0) {
            logger.info("Finished Catalog migration");
        } else {
            throw new CatalogException("Error migrating catalog database!");
        }
    }

}
