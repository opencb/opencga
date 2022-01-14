package org.opencb.opencga.app.cli.main.processors;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.app.cli.main.CommandLineUtils;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;


public class CliProcessor extends AbstractProcessor {


    private static String[] getUserPasswordArgs(String[] args, String s) {
        for (int i = 0; i < args.length; i++) {
            if (s.equals(args[i])) {
                args[i] = "--password";
                break;
            }
        }
        return args;
    }


    public void parseParams(String[] args) throws CatalogAuthenticationException {
        CommandLineUtils.printDebug("Executing " + String.join(" ", args));
        //login The first if clause is for scripting login method and the else clause is for the shell login
        if (isNotHelpCommand(args)) {
            if (args.length > 3 && "users".equals(args[0]) && "login".equals(args[1]) && ArrayUtils.contains(args, "--user-password")) {
                args = getUserPasswordArgs(args, "--user-password");
            }
        }

    }
}
