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

    }

    @Parameters(commandNames = {"update-database-prefix"}, commandDescription = "Update database prefix on the variant storage")
    public class UpdateDatabasePrefix extends AdminCliOptionsParser.CatalogDatabaseCommandOptions {

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
