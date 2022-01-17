package org.opencb.opencga.app.cli.main.processors;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.app.cli.main.CommandLineUtils;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.client.exceptions.ClientException;

import java.io.IOException;

import static org.opencb.commons.utils.PrintUtils.*;


public class ShellProcessor extends AbstractProcessor {

    public ShellProcessor() {
        super();
    }

    public boolean parseParams(String[] args) throws CatalogAuthenticationException {
        CommandLineUtils.printDebug("Executing " + String.join(" ", args));
        if (ArrayUtils.contains(args, "--host")) {
            printDebug("To change host you must exit the shell and launch it again with the --host parameter.");
            return false;
        } else {
            try {
                args = ArrayUtils.addAll(args, "--host",
                        CliSessionManager.getShell().getClientConfiguration().getCurrentHost().getName());
            } catch (ClientException e) {
                printError("Error loading current host.", e);
            }
        }

        if (args.length == 1 && "exit".equals(args[0])) {
            println("\nThanks for using OpenCGA. See you soon.\n\n", Color.YELLOW);
            System.exit(0);
        }

        if (args.length == 3 && "use".equals(args[0]) && "study".equals(args[1])) {
            CliSessionManager.getInstance().setValidatedCurrentStudy(args[2], CliSessionManager.getShell());
            return false;
        }

        //login The first if clause is for scripting login method and the else clause is for the shell login
        if (isNotHelpCommand(args)) {
            if (args.length > 3 && "users".equals(args[0]) && "login".equals(args[1]) && ArrayUtils.contains(args, "--user-password")) {
                char[] passwordArray =
                        console.readPassword(format("\nEnter your password: ", Color.GREEN));
                args = ArrayUtils.addAll(args, "--password", new String(passwordArray));

            }
        }
        return true;
    }


    public void executeCommand(OpencgaCommandExecutor commandExecutor, OpencgaCliOptionsParser cliOptionsParser) {
        if (commandExecutor != null) {
            try {
                commandExecutor.execute();
                CliSessionManager.getInstance().updateSession(commandExecutor);
                CliSessionManager.getInstance().loadSessionStudies(commandExecutor);
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
