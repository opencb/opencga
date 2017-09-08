package org.opencb.opencga.app.cli.admin.executors;

import org.opencb.opencga.app.cli.admin.options.MigrationCommandOptions;

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
                v130();
                break;
            default:
                logger.error("Subcommand '{}' not valid", subCommandString);
                break;
        }
    }

    private void v130() throws Exception {
        logger.warn("MIGRATING v1.3.0");
    }
}
