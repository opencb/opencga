package org.opencb.opencga.app.cli.admin.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;

/**
 * Created on 08/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Parameters(commandNames = {"migration"}, commandDescription = "Migrate internal data models to upgrade OpenCGA version")
public class MigrationCommandOptions extends GeneralCliOptions {

    private final MigrateV1_3_0CommandOptions migrateV130CommandOptions;
    private final AdminCliOptionsParser.AdminCommonCommandOptions commonOptions;

    public MigrationCommandOptions(JCommander jCommander, AdminCliOptionsParser.AdminCommonCommandOptions commonOptions) {
        super(jCommander);
        this.commonOptions = commonOptions;
        this.migrateV130CommandOptions = new MigrateV1_3_0CommandOptions();
    }

    @Parameters(commandNames = {"v1.3.0"}, commandDescription = "Migrate OpenCGA from version 1.2.x to 1.3.0")
    public class MigrateV1_3_0CommandOptions extends AdminCliOptionsParser.CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCliOptionsParser.AdminCommonCommandOptions commonOptions = MigrationCommandOptions.this.commonOptions;

    }

    public MigrateV1_3_0CommandOptions getMigrateV130CommandOptions() {
        return migrateV130CommandOptions;
    }

    public AdminCliOptionsParser.AdminCommonCommandOptions getCommonOptions() {
        return commonOptions;
    }
}
