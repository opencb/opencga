package org.opencb.opencga.app.cli.admin;

/**
 * Created by wasim on 08/06/17.
 */

import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser.MetaCommandOptions;
import org.opencb.opencga.catalog.config.Admin;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;

public class MetaCommandExecutor extends AdminCommandExecutor {
    private MetaCommandOptions metaCommandOptions;

    public MetaCommandExecutor(MetaCommandOptions metaCommandOptions) {
        super(metaCommandOptions.commonOptions);
        this.metaCommandOptions = metaCommandOptions;
    }

    public void execute() throws Exception {
        this.logger.debug("Executing Meta command line");
        String subCommandString = metaCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "update":
                insertUpdatedAAdmin();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }
    }

    private void insertUpdatedAAdmin() throws CatalogException {

        if (this.metaCommandOptions.metaKeyCommandOptions.updateSecretKey != null ||
                this.metaCommandOptions.metaKeyCommandOptions.algorithm != null) {

            CatalogManager catalogManager = new CatalogManager(configuration);
            Admin admin = new Admin();

            if (this.metaCommandOptions.metaKeyCommandOptions.updateSecretKey != null) {
                admin.setSecretKey(this.metaCommandOptions.metaKeyCommandOptions.updateSecretKey);
            }

            if (this.metaCommandOptions.metaKeyCommandOptions.algorithm != null) {
                admin.setAlgorithm(this.metaCommandOptions.metaKeyCommandOptions.algorithm);
            }

            catalogManager.insertUpdatedAdmin(admin);
        }

    }

}
