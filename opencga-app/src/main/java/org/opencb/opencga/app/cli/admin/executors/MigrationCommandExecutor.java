package org.opencb.opencga.app.cli.admin.executors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.app.cli.admin.executors.migration.AnnotationSetMigration;
import org.opencb.opencga.app.cli.admin.executors.migration.NewVariantMetadataMigration;
import org.opencb.opencga.app.cli.admin.options.MigrationCommandOptions;
import org.opencb.opencga.catalog.auth.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.managers.CatalogManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Paths;

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
                String sessionId = catalogManager.getUserManager().login("admin", options.commonOptions.adminPassword);

                // Catalog
                String basePath = appHome + "/migration/v1.3.0/";

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


                // Storage

                new NewVariantMetadataMigration(storageConfiguration, catalogManager, options).migrate(sessionId);
            }
        }
    }

    private void v1_4_0() throws Exception {
        logger.info("MIGRATING v1.4.0");
        MigrationCommandOptions.MigrateV1_4_0CommandOptions options = migrationCommandOptions.getMigrateV140CommandOptions();

        setCatalogDatabaseCredentials(options, options.commonOptions);

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            catalogManager.getUserManager().login("admin", options.commonOptions.adminPassword);

            // Catalog
            String basePath = appHome + "/migration/v1.4.0/";

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

            // Migrate annotationSets
            new AnnotationSetMigration(catalogManager.getConfiguration()).migrate();
        }
    }


}
