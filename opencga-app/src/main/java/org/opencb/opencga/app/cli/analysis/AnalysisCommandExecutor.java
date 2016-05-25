package org.opencb.opencga.app.cli.analysis;

import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.storage.core.StorageManagerFactory;

/**
 * Created on 03/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AnalysisCommandExecutor extends CommandExecutor {

    public AnalysisCommandExecutor(GeneralCliOptions.CommonCommandOptions options) {
        super(options);
    }

}
