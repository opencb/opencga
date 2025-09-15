package org.opencb.opencga.app.cli.admin.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;

/**
 * Created on 08/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Parameters(commandNames = {"storage"}, commandDescription = "Generic storage engine options")
public class StorageCommandOptions extends GeneralCliOptions {

    private final StatusCommandOptions statusCommandOptions;
    private final UpdateDatabasePrefix updateDatabasePrefix;
    private final AdminCliOptionsParser.AdminCommonCommandOptions commonOptions;

    public StorageCommandOptions(JCommander jCommander, AdminCliOptionsParser.AdminCommonCommandOptions commonOptions) {
        super(jCommander);
        this.commonOptions = commonOptions;
        this.statusCommandOptions = new StatusCommandOptions();
        this.updateDatabasePrefix = new UpdateDatabasePrefix();
    }


    @Parameters(commandNames = {"status"}, commandDescription = "Obtain storage status")
    public class StatusCommandOptions extends AdminCliOptionsParser.CatalogDatabaseCommandOptions {

        @ParametersDelegate
//        public AdminCliOptionsParser.AdminCommonCommandOptions commonOptions = StorageCommandOptions.this.commonOptions;
        public AdminCliOptionsParser.IgnorePasswordCommonCommandOptions commonOptions =
                new AdminCliOptionsParser.IgnorePasswordCommonCommandOptions(getCommonOptions().commonOptions);

        @Parameter(names = {"--organization"}, description = "Organization id. If empty, will run for all organizations.", arity = 1)
        public String organizationId;

    }

    @Parameters(commandNames = {"update-database-prefix"}, commandDescription = "Update database prefix of the variant storage stored in "
            + "catalog. Does not modify the actual database names, just the database names stored in catalog.")
    public class UpdateDatabasePrefix extends AdminCliOptionsParser.CatalogDatabaseCommandOptions {

        @Parameter(names = {"--organization"}, description = "Organization id. If empty, will run for all organizations.", arity = 1)
        public String organizationId;

        @Parameter(names = {"--projects"}, description = "List of projects")
        public String projects;

        @Parameter(names = {"--project"}, hidden = true, description = "hidden alias")
        public void setProject(String p) {
            projects = p;
        };

        @Parameter(names = {"--db-prefix"}, hidden = true, description = "Database prefix. If empty, use the system db prefix.")
        public String dbPrefix;

        @Parameter(names = {"--bioformat"}, hidden = true, description = "")
        public String bioformat = "VARIANT";

        @Parameter(names = {"--projects-without-storage"}, arity = 0, description = "Only update projects that don't exist on the VariantStorage. This param can only be used with bioformat=VARIANT")
        public boolean projectsWithoutStorage;

        @Parameter(names = {"--projects-with-storage"}, arity = 0, description = "Only update projects that exist on the VariantStorage. This param can only be used with bioformat=VARIANT")
        public boolean projectsWithStorage;

        @Parameter(names = {"--projects-with-undefined-dbname"}, arity = 0, description = "Only update projects with undefined dbname")
        public boolean projectsWithUndefinedDBName;

        @ParametersDelegate
//        public AdminCliOptionsParser.AdminCommonCommandOptions commonOptions = StorageCommandOptions.this.commonOptions;
        public AdminCliOptionsParser.IgnorePasswordCommonCommandOptions commonOptions =
                new AdminCliOptionsParser.IgnorePasswordCommonCommandOptions(getCommonOptions().commonOptions);

    }

    public StatusCommandOptions getStatusCommandOptions() {
        return statusCommandOptions;
    }

    public UpdateDatabasePrefix getUpdateDatabasePrefix() {
        return updateDatabasePrefix;
    }

    public AdminCliOptionsParser.AdminCommonCommandOptions getCommonOptions() {
        return commonOptions;
    }
}
