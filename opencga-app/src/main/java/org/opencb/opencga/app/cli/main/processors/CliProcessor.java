package org.opencb.opencga.app.cli.main.processors;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.app.cli.main.CommandLineUtils;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

import java.io.IOException;


public class CliProcessor extends AbstractProcessor {


    public CliProcessor() {
        super();
    }

    private static String[] normalizePasswordArgs(String[] args, String s) {
        for (int i = 0; i < args.length; i++) {
            if (s.equals(args[i])) {
                args[i] = "--password";
                break;
            }
        }
        return args;
    }


    public String[] parseParams(String[] args) throws CatalogAuthenticationException {
        CommandLineUtils.printDebug("Executing " + String.join(" ", args));
        if (isNotHelpCommand(args)) {
            if (ArrayUtils.contains(args, "--user-password")) {
                return normalizePasswordArgs(args, "--user-password");
            }
        }
        return args;
    }


    public void executeCommand(OpencgaCommandExecutor commandExecutor, OpencgaCliOptionsParser cliOptionsParser) {
        if (commandExecutor != null) {
            try {
                commandExecutor.execute();
                commandExecutor.getSessionManager().saveCliSession();
            } catch (IOException e) {
                CommandLineUtils.printError("Could not set the default study", e);
                System.exit(1);
            } catch (Exception ex) {
                CommandLineUtils.printError("Execution error: " + ex.getMessage(), ex);
                System.exit(1);
            }
        } else {
            cliOptionsParser.printUsage();
            System.exit(1);
        }
    }
}
