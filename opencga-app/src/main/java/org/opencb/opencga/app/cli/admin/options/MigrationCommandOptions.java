package org.opencb.opencga.app.cli.admin.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.catalog.migration.Migration;

import java.util.List;

/**
 * Created on 08/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Parameters(commandNames = {"migration"}, commandDescription = "Migrate internal data models to upgrade OpenCGA version")
public class MigrationCommandOptions extends GeneralCliOptions {

    private final SearchCommandOptions searchCommandOptions;
    private final RunCommandOptions runCommandOptions;
    private final RunManualCommandOptions runManualCommandOptions;
    private final AdminCliOptionsParser.AdminCommonCommandOptions commonOptions;

    public MigrationCommandOptions(JCommander jCommander, AdminCliOptionsParser.AdminCommonCommandOptions commonOptions) {
        super(jCommander);
        this.commonOptions = commonOptions;
        this.searchCommandOptions = new SearchCommandOptions();
        this.runCommandOptions = new RunCommandOptions();
        this.runManualCommandOptions = new RunManualCommandOptions();
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search for migrations")
    public class SearchCommandOptions extends AdminCliOptionsParser.CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCliOptionsParser.AdminCommonCommandOptions commonOptions = MigrationCommandOptions.this.commonOptions;

        @Parameter(names = {"--status"}, description = "Filter migrations by status. PENDING, ERROR, DONE")
        public List<String> status;

        @Parameter(names = {"--domain"}, description = "Select migration domain, either CATALOG or STORAGE")
        public List<Migration.MigrationDomain> domain;

        @Parameter(names = {"--version"}, description = "Migration version")
        public String version;
    }
    
    @Parameters(commandNames = {"run"}, commandDescription = "Run migrations")
    public class RunCommandOptions extends AdminCliOptionsParser.CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCliOptionsParser.AdminCommonCommandOptions commonOptions = MigrationCommandOptions.this.commonOptions;

        @Parameter(names = {"--domain"}, description = "Run migrations of the chosen domain only [CATALOG, STORAGE]")
        public Migration.MigrationDomain domain;

        @Parameter(names = {"--version"}, description = "Run all pending migrations up to this version number")
        public String version;

    }

    @Parameters(commandNames = {"run-manual"}, commandDescription = "Manually run a migration")
    public class RunManualCommandOptions extends AdminCliOptionsParser.CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCliOptionsParser.AdminCommonCommandOptions commonOptions = MigrationCommandOptions.this.commonOptions;

        @Parameter(names = {"--id"}, description = "Migration ID to run", required = true)
        public String id;

        @Parameter(names = {"--version"}, description = "Migration version")
        public String version;

        @Parameter(names = {"--force"}, description = "Force migration run even if it's on status DONE, ON_HOLD or REDUNDANT", arity = 0)
        public boolean force;

    }

    public SearchCommandOptions getSearchCommandOptions() {
        return searchCommandOptions;
    }

    public RunCommandOptions getRunCommandOptions() {
        return runCommandOptions;
    }

    public RunManualCommandOptions getRunManualCommandOptions() {
        return runManualCommandOptions;
    }

    public AdminCliOptionsParser.AdminCommonCommandOptions getCommonOptions() {
        return commonOptions;
    }
}
