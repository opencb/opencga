package org.opencb.opencga.app.cli.main.utils;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.main.OpencgaMain;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.opencb.commons.utils.PrintUtils.*;

public class LoginUtils {

    public static String[] forceLogin(String[] args) {
        String user = System.console().readLine(format("\nEnter your user: ", PrintUtils.Color.GREEN)).trim();
        CommandLineUtils.debug("Login user " + user);
        return loginUser(args, user);
    }

    public static String[] loginUser(String[] args, String user) {
        String password = "";
        if (System.console() != null) {
            char[] passwordArray = System.console().readPassword(format("\nEnter your password: ", PrintUtils.Color.GREEN));
            password = new String(passwordArray).trim();
        } else {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    System.in));
            try {
                password = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            CommandLineUtils.debug("Console null ");
        }
        if (CommandLineUtils.isValidUser(user)) {
            args = ArrayUtils.addAll(args, "-u", user);
            args = ArrayUtils.addAll(args, "--password", password);
            CommandLineUtils.debug(ArrayUtils.toString(args));
        } else {
            println(PrintUtils.format("Invalid user name: ", Color.RED) + PrintUtils.format(user, Color.DEFAULT));
            printUsage(args);
        }

        return args;
    }


    public static String[] parseLoginCommand(String[] args) {
        //adds in position 0 command "users"

        CommandLineUtils.debug("LOGIN COMMAND: " + CommandLineUtils.argsToString(args));
        if ("login".equals(args[0]) && (ArrayUtils.contains(args, "-u") || ArrayUtils.contains(args, "-u"))) {
            printUsage(args);
            return null;
        }
        if ("login".equals(args[0]) && (args[1].startsWith("-")) && (!"--host".equals(args[1]))) {
            printUsage(args);
            return null;
        }

        args = ArrayUtils.addAll(new String[]{"users"}, args);

        //case opencga.sh login OR [opencga][demo@project:study]<demo/>login
        if (args.length == 2 && "login".equals(args[1])) {
            return forceLogin(args);
        }

        //CASES
        //case opencga.sh login --host ...... OR [opencga][demo@project:study]<demo/>login --host ......
        //case opencga.sh login user1 [.....] OR [opencga][demo@project:study]<demo/>login user1 [.....]
        if (args.length > 2 && "login".equals(args[1])) {
            if (args[2].startsWith("-")) {
                return forceLogin(args);
            } else {
                String user = args[2];
                args = ArrayUtils.remove(args, 2);
                return loginUser(args, user);
            }
        }

        return args;
    }

    private static void printUsage(String[] args) {

        PrintUtils.println("");
        if (!isHelp(args)) {
            PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("Invalid Command: ", CommandLineUtils.argsToString(args)));
        }
        if (OpencgaMain.isShellMode()) {
            PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("Usage:", "       login [user] [--host host]"));
        } else {
            PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("Usage:", "       opencga.sh login [user] [--host host]"));
        }
        PrintUtils.println("");
    }

    private static boolean isHelp(String[] args) {
        return ArrayUtils.contains(args, "--help")
                || ArrayUtils.contains(args, "-h")
                || ArrayUtils.contains(args, "?")
                || ArrayUtils.contains(args, "help");
    }


}
