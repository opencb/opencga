package org.opencb.opencga.app.cli.main.processors;

import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.parser.ParamParser;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;

public class CliCommandProcessor extends AbstractCommandProcessor {


    public CliCommandProcessor(ParamParser parser) {
        super(parser);
    }

    public void processCommand(OpencgaCommandExecutor commandExecutor, OpencgaCliOptionsParser cliOptionsParser) {
        if (commandExecutor != null) {
            try {
                commandExecutor.execute();
                commandExecutor.getSessionManager().saveSession();
            } catch (Exception ex) {
                CommandLineUtils.error("Execution error: " + ex.getMessage(), ex);
                System.exit(1);
            }
        } else {
            cliOptionsParser.printUsage();
            System.exit(1);
        }
    }

}
