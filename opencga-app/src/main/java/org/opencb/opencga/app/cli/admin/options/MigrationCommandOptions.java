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
    private final MigrateV2_0_0CommandOptions migrateV200CommandOptions;
    private final MigrateV2_0_1CommandOptions migrateV201CommandOptions;
    private final AdminCliOptionsParser.AdminCommonCommandOptions commonOptions;

    public MigrationCommandOptions(JCommander jCommander, AdminCliOptionsParser.AdminCommonCommandOptions commonOptions) {
        super(jCommander);
        this.commonOptions = commonOptions;
        this.migrateV130CommandOptions = new MigrateV1_3_0CommandOptions();
        this.migrateV140CommandOptions = new MigrateV1_4_0CommandOptions();
        this.migrateV200CommandOptions = new MigrateV2_0_0CommandOptions();
        this.migrateV201CommandOptions = new MigrateV2_0_1CommandOptions();
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

    @Parameters(commandNames = {"v2.0.0"}, commandDescription = "Migrate OpenCGA from version 1.4.2 to 2.0.0")
    public class MigrateV2_0_0CommandOptions extends AdminCliOptionsParser.CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCliOptionsParser.AdminCommonCommandOptions commonOptions = MigrationCommandOptions.this.commonOptions;

        @Parameter(names = {"--job-directory"}, description = "This new OpenCGA version requires a folder where all the analyses will be"
                + " executed from. Please, specify which folder you will be using. Mandatory parameter when migrating ALL or RC1."
                + " WARNING: The folder typed here will necessarily need to be exactly the same one specified in the configuration file !",
                arity = 1)
        public String jobFolder;

        @Parameter(names = {"--what"}, description = "Select to which version migrate. To get to the stable version, it is mandatory "
                + "migrating to RC1 and RC2 first !!. Options: ALL, RC1, RC2, STABLE, VARIANT_STORAGE")
        public MigrateRC what = MigrateRC.ALL;
    }

    @Parameters(commandNames = {"v2.0.1"}, commandDescription = "Migrate OpenCGA from version 2.0.0 to 2.0.1")
    public class MigrateV2_0_1CommandOptions extends AdminCliOptionsParser.CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCliOptionsParser.AdminCommonCommandOptions commonOptions = MigrationCommandOptions.this.commonOptions;

    }

    public enum MigrateRC {
        ALL,
        RC1,
        RC2,
        STABLE,
        VARIANT_STORAGE
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

    public MigrateV2_0_0CommandOptions getMigrateV200CommandOptions() {
        return migrateV200CommandOptions;
    }

    public MigrateV2_0_1CommandOptions getMigrateV201CommandOptions() {
        return migrateV201CommandOptions;
    }

    public AdminCliOptionsParser.AdminCommonCommandOptions getCommonOptions() {
        return commonOptions;
    }
}
