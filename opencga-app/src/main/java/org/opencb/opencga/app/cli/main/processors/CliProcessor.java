package org.opencb.opencga.app.cli.main.processors;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.app.cli.main.CommandLineUtils;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

import java.io.IOException;


public class CliProcessor extends AbstractProcessor {


    public CliProcessor() {
        super();
    }

    private static String[] getUserPasswordArgs(String[] args, String s) {
        for (int i = 0; i < args.length; i++) {
            if (s.equals(args[i])) {
                args[i] = "--password";
                break;
            }
        }
        return args;
    }


    public boolean parseParams(String[] args) throws CatalogAuthenticationException {
        CommandLineUtils.printDebug("Executing " + String.join(" ", args));
        //login The first if clause is for scripting login method and the else clause is for the shell login
        if (isNotHelpCommand(args)) {
            if (args.length > 3 && "users".equals(args[0]) && "login".equals(args[1]) && ArrayUtils.contains(args, "--user-password")) {
                args = getUserPasswordArgs(args, "--user-password");
            }
        }
        return true;
    }


    public void executeCommand(OpencgaCommandExecutor commandExecutor, OpencgaCliOptionsParser cliOptionsParser) {
        if (commandExecutor != null) {
            try {
                commandExecutor.execute();
                CliSessionManager.getInstance().updateSession(commandExecutor);
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
