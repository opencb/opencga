package org.opencb.opencga.app.cli.admin.executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.admin.options.MigrationCommandOptions;
import org.opencb.opencga.app.cli.main.io.Table;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationManager;
import org.opencb.opencga.catalog.migration.MigrationRun;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
            case "search":
                search();
                break;
            case "run":
                run();
                break;
            case "run-manual":
                runManual();
                break;
            default:
                logger.error("Subcommand '{}' not valid", subCommandString);
                break;
        }
    }

    private void search() throws Exception {
        MigrationCommandOptions.SearchCommandOptions options = migrationCommandOptions.getSearchCommandOptions();
        setCatalogDatabaseCredentials(options, options.commonOptions);

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            String token = catalogManager.getUserManager().loginAsAdmin(options.commonOptions.adminPassword).getToken();

            List<Pair<Migration, MigrationRun>> rows = catalogManager.getMigrationManager()
                    .getMigrationRuns(options.version, options.domain, options.status, token);

            if (options.commonOptions.commonOptions.outputFormat.toLowerCase().contains("json")) {
                for (Pair<Migration, MigrationRun> pair : rows) {
                    System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(pair));
                }
            } else {
                Table<Pair<Migration, MigrationRun>> table = new Table<Pair<Migration, MigrationRun>>(Table.PrinterType.JANSI)
                        .addColumn("ID", p -> p.getKey().id(), 50)
                        .addColumn("Description", p -> p.getKey().description(), 50)
                        .addColumnEnum("Domain", p -> p.getKey().domain())
                        .addColumn("Version", p -> p.getKey().version())
                        .addColumnEnum("Language", p -> p.getKey().language())
                        .addColumn("Manual", p -> Boolean.toString(p.getKey().manual()))
                        .addColumnNumber("Patch", p -> p.getKey().patch())
                        .addColumn("Status", MigrationCommandExecutor::getMigrationStatus)
                        .addColumnNumber("RunPatch", p -> p.getValue().getPatch())
                        .addColumn("ExecutionTime", p -> p.getValue().getStart() + " " + TimeUtils.durationToString(ChronoUnit.MILLIS.between(
                                p.getValue().getStart().toInstant(),
                                p.getValue().getEnd().toInstant())))
                        .addColumn("Exception", p -> p.getValue().getException());
                table.printTable(rows);
            }
        }
    }

    private static String getMigrationStatus(Pair<Migration, MigrationRun> p) {
        if (p.getValue() == null) {
            return "PENDING";
        } else {
            return p.getValue().getStatus().name();
        }
    }

    private void run() throws CatalogException, IOException {
        MigrationCommandOptions.RunCommandOptions options = migrationCommandOptions.getRunCommandOptions();
        setCatalogDatabaseCredentials(options, options.commonOptions);

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            String token = catalogManager.getUserManager().loginAsAdmin(options.commonOptions.adminPassword).getToken();

            String version = parseVersion(options.version);

            MigrationManager migrationManager = catalogManager.getMigrationManager();
            migrationManager.runMigration(version, options.domain, options.language, appHome, token);
        }
    }

    private void runManual() throws Exception {
        MigrationCommandOptions.RunManualCommandOptions options = migrationCommandOptions.getRunManualCommandOptions();
        setCatalogDatabaseCredentials(options, options.commonOptions);

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            String token = catalogManager.getUserManager().loginAsAdmin(options.commonOptions.adminPassword).getToken();

            catalogManager.getMigrationManager().runManualMigration(parseVersion(options.version), options.id, Paths.get(appHome),
                    options.force, new ObjectMap(options.commonOptions.commonOptions.params), token);
        }
    }

    private String parseVersion(String version) {
        if (StringUtils.isEmpty(version)) {
            return getDefaultVersion();
        } else {
            // Remove "v" (v1.1.0 -> 1.1.0)
            version = StringUtils.removeStart(version, "v");
            return version;
        }
    }

    public static String getDefaultVersion() {
        String version;
        version = GitRepositoryState.get().getBuildVersion();
        // Remove extra information
        version = version.split("-")[0];
        return version;
    }

}
