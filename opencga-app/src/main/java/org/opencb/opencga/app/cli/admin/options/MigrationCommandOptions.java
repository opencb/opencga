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

    private final Migrate130CommandOptions migrate130CommandOptions;
    private final AdminCliOptionsParser.AdminCommonCommandOptions commonOptions;

    public MigrationCommandOptions(JCommander jCommander, AdminCliOptionsParser.AdminCommonCommandOptions commonOptions) {
        super(jCommander);
        this.commonOptions = commonOptions;
        this.migrate130CommandOptions = new Migrate130CommandOptions();
    }

    @Parameters(commandNames = {"v1.3.0"}, commandDescription = "Migrate OpenCGA from version 1.2.x to 1.3.0")
    public class Migrate130CommandOptions extends AdminCliOptionsParser.CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCliOptionsParser.AdminCommonCommandOptions commonOptions = MigrationCommandOptions.this.commonOptions;

    }

    public Migrate130CommandOptions getMigrate130CommandOptions() {
        return migrate130CommandOptions;
    }

    public AdminCliOptionsParser.AdminCommonCommandOptions getCommonOptions() {
        return commonOptions;
    }
}
