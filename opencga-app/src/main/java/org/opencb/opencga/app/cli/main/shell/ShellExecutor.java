package org.opencb.opencga.app.cli.main.shell;

import org.opencb.commons.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.impl.CommandExecutorImpl;

import java.util.logging.Level;

public class ShellExecutor extends CommandExecutorImpl {


    public ShellExecutor(GeneralCliOptions.CommonCommandOptions options, boolean loadClientConfiguration) {
        super(options, loadClientConfiguration);
    }

    public ShellExecutor(GeneralCliOptions.CommonCommandOptions options, boolean loadClientConfiguration, Level logLevel) {
        super(options, loadClientConfiguration);
        getSessionManager().setLogLevel(String.valueOf(logLevel));
    }

    @Override
    public void execute() throws Exception {

    }
}
