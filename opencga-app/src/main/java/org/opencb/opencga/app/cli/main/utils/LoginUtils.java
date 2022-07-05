package org.opencb.opencga.app.cli.main.utils;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.commons.app.cli.main.utils.CommandLineUtils;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.main.OpencgaCommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.opencb.commons.utils.PrintUtils.*;

public class LoginUtils {

    private static final Logger logger = LoggerFactory.getLogger(LoginUtils.class);

    public static String[] parseLoginCommand(String[] args) {
        logger.debug("LOGIN COMMAND: " + CommandLineUtils.argsToString(args));

        // 1. Make sure first argument is 'login'
        if (!args[0].equals("login")) {
            printUsage(args);
            return null;
        }

        // 2. There are only four valid lengths and formats accepted:
        //      1. login
        //      2. login imedina
        //      3. login --host host
        //      4. login imedina --host host
        switch (args.length) {
            case 2:
                if (args[1].startsWith("-")) {
                    printUsage(args);
                    return null;
                }
                break;
            case 3:
                if (!args[1].equals("--host")) {
                    printUsage(args);
                    return null;
                }
                break;
            case 4:
                if (args[1].startsWith("-") || !args[2].equals("--host")) {
                    printUsage(args);
                    return null;
                }
                break;
            default:
                if (args.length > 4) {
                    printUsage(args);
                    return null;
                }
                break;
        }

        // Add 'users' as first argument
        args = ArrayUtils.addFirst(args, "users");

        if (args.length == 2 || args.length == 4) {
            // We need to ask for the user in 2 of the 4 possible cases here:
            //  1. users login
            //  2. users login --host host
            return loginUser(args);
        } else {
            // Two possible cases here:
            //  1. users login imedina
            //  2. users login imedina --host host
            String user = args[2];
            args = ArrayUtils.remove(args, 2);
            return loginUser(args, user);
        }
    }

    private static String[] loginUser(String[] args) {
        String user = System.console().readLine(format("\nEnter your user: ", PrintUtils.Color.GREEN)).trim();
        logger.debug("Login user " + user);
        return loginUser(args, user);
    }

    private static String[] loginUser(String[] args, String user) {
        String password = "";
        if (System.console() != null) {
            char[] passwordArray = System.console().readPassword(format("\nEnter your password: ", PrintUtils.Color.GREEN));
            password = new String(passwordArray).trim();
        } else {
            logger.debug("System.console() is null");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                password = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (CommandLineUtils.isValidUser(user)) {
            args = ArrayUtils.addAll(args, "-u", user, "--password", password);
            logger.debug(ArrayUtils.toString(args));
        } else {
            println(PrintUtils.format("Invalid user name: ", Color.RED) + PrintUtils.format(user, Color.DEFAULT));
            printUsage(args);
        }

        return args;
    }

    private static void printUsage(String[] args) {
        PrintUtils.println("");
        if (!isHelp(args)) {
            PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("Invalid Command: ", CommandLineUtils.argsToString(args)));
        }
        if (OpencgaCommandLine.isShellMode()) {
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

