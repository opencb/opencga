package org.opencb.opencga.app.cli.admin;

/**
 * Created by wasim on 08/06/17.
 */
import org.opencb.opencga.app.cli.admin.AdminCommandExecutor;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser.MetaCommandOptions;
import org.opencb.opencga.catalog.db.mongodb.MetaMongoDBAdaptor;
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
            case "key":
                insertSecretKey();
                break;
            default:
                logger.error("Subcommand not valid" + subCommandString);
                break;
        }
    }

    private void insertSecretKey() throws CatalogException {
        if(this.metaCommandOptions.metaKeyCommandOptions.updateSecretKey != null) {
            CatalogManager catalogManager = new CatalogManager(configuration);
            catalogManager.insertUpdatedSecretKey(this.metaCommandOptions.metaKeyCommandOptions.updateSecretKey);
        }

    }
}
