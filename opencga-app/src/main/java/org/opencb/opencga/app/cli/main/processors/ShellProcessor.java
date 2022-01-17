package org.opencb.opencga.app.cli.main.processors;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.app.cli.main.CommandLineUtils;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

import static org.opencb.commons.utils.PrintUtils.*;


public class ShellProcessor extends AbstractProcessor {


    public void parseParams(String[] args) throws CatalogAuthenticationException {
        CommandLineUtils.printDebug("Executing " + String.join(" ", args));

        if (args.length == 1 && "exit".equals(args[0])) {
            println("\nThanks for using OpenCGA. See you soon.\n\n", Color.YELLOW);
            System.exit(0);
        }

        if (args.length == 3 && "use".equals(args[0]) && "study".equals(args[1])) {
            CliSessionManager.getInstance().setValidatedCurrentStudy(args[2]);
            return;
        }
      
        //login The first if clause is for scripting login method and the else clause is for the shell login
        if (isNotHelpCommand(args)) {
            if (args.length > 3 && "users".equals(args[0]) && "login".equals(args[1]) && ArrayUtils.contains(args, "--user-password")) {
                char[] passwordArray =
                        console.readPassword(format("\nEnter your password: ", Color.GREEN));
                args = ArrayUtils.addAll(args, "--password", new String(passwordArray));

            }
        }

    }


}
