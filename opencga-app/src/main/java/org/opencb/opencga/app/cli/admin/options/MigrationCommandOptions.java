package org.opencb.opencga.app.cli.admin.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;

import java.util.List;

/**
 * Created on 08/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Parameters(commandNames = {"migration"}, commandDescription = "Migrate internal data models to upgrade OpenCGA version")
public class MigrationCommandOptions extends GeneralCliOptions {

    private final MigrateV1_3_0CommandOptions migrateV130CommandOptions;
    private final MigrateV1_4_0CommandOptions migrateV140CommandOptions;
    private final AdminCliOptionsParser.AdminCommonCommandOptions commonOptions;

    public MigrationCommandOptions(JCommander jCommander, AdminCliOptionsParser.AdminCommonCommandOptions commonOptions) {
        super(jCommander);
        this.commonOptions = commonOptions;
        this.migrateV130CommandOptions = new MigrateV1_3_0CommandOptions();
        this.migrateV140CommandOptions = new MigrateV1_4_0CommandOptions();
    }

    @Parameters(commandNames = {"v1.3.0"}, commandDescription = "Migrate OpenCGA from version 1.2.x to 1.3.0")
    public class MigrateV1_3_0CommandOptions extends AdminCliOptionsParser.CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCliOptionsParser.AdminCommonCommandOptions commonOptions = MigrationCommandOptions.this.commonOptions;

        @Parameter(names = {"--file-backup"}, description = "Create a backup for all migrated variant metadata files")
        public boolean createBackup;

        @Parameter(names = {"--skip-disk-files"}, description = "Do not migrate VariantSource files from disk.")
        public boolean skipDiskFiles;

        @Parameter(names = {"--files"}, description = "VariantSource files to migrate into VariantFileMetadata. Don't do any operation in catalog.", variableArity = true)
        public List<String> files;

    }

    @Parameters(commandNames = {"v1.4.0"}, commandDescription = "Migrate OpenCGA from version 1.3.x to 1.4.0")
    public class MigrateV1_4_0CommandOptions extends AdminCliOptionsParser.CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCliOptionsParser.AdminCommonCommandOptions commonOptions = MigrationCommandOptions.this.commonOptions;

        @Parameter(names = {"--what"}, description = "Select which parts will be migrated. Options: ALL, CATALOG, STORAGE, ANNOTATIONS, "
                + "CATALOG_NO_ANNOTATIONS")
        public Migrate what = Migrate.ALL;
    }

    public enum Migrate {
        ALL,
        CATALOG,
        STORAGE,
        ANNOTATIONS,
        CATALOG_NO_ANNOTATIONS
    }

    public MigrateV1_3_0CommandOptions getMigrateV130CommandOptions() {
        return migrateV130CommandOptions;
    }

    public MigrateV1_4_0CommandOptions getMigrateV140CommandOptions() {
        return migrateV140CommandOptions;
    }

    public AdminCliOptionsParser.AdminCommonCommandOptions getCommonOptions() {
        return commonOptions;
    }
}
