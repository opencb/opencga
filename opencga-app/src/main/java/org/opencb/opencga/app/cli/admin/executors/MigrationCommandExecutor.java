package org.opencb.opencga.app.cli.admin.executors;

import org.opencb.opencga.app.cli.admin.executors.migration.NewVariantMetadataMigration;
import org.opencb.opencga.app.cli.admin.options.MigrationCommandOptions;
import org.opencb.opencga.catalog.managers.CatalogManager;

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
                new NewVariantMetadataMigration(storageConfiguration, catalogManager, options).migrate(sessionId);
            }
        }
    }

}
