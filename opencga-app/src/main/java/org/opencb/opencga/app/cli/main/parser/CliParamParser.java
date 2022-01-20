package org.opencb.opencga.app.cli.main.parser;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;


public class CliParamParser implements ParamParser {


    public CliParamParser() {
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
        CommandLineUtils.printLog("Executing " + String.join(" ", args));
        if (isNotHelpCommand(args)) {
            if (ArrayUtils.contains(args, "--user-password")) {
                return normalizePasswordArgs(args, "--user-password");
            }
        }
        return args;
    }


    protected boolean isNotHelpCommand(String[] args) {
        return !ArrayUtils.contains(args, "--help") && !ArrayUtils.contains(args, "-h");
    }


}
