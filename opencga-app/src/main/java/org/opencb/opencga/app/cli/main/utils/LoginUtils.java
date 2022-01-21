package org.opencb.opencga.app.cli.main.utils;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.commons.utils.PrintUtils;

import static org.opencb.commons.utils.PrintUtils.*;

public class LoginUtils {

    public static String[] forceLogin(String[] args) {
        String user = System.console().readLine(format("\nEnter your user: ", PrintUtils.Color.GREEN)).trim();
        CommandLineUtils.debug("Login user " + user);
        return loginUser(args, user);
    }

    public static String[] loginUser(String[] args, String user) {
        char[] passwordArray = System.console().readPassword(format("\nEnter your password: ", PrintUtils.Color.GREEN));
        if (CommandLineUtils.isValidUser(user)) {
            args = ArrayUtils.addAll(args, "-u", user);
            args = ArrayUtils.addAll(args, "--password", new String(passwordArray).trim());
            CommandLineUtils.debug(ArrayUtils.toString(args));
        } else {
            println(PrintUtils.format("Invalid user name: ", Color.RED) + PrintUtils.format(user, Color.DEFAULT));
        }
        return args;
    }


    public static String[] parseLoginCommand(String[] args) {
        //adds in position 0 command "users"
        args = ArrayUtils.addAll(new String[]{"users"}, args);

        //case opencga.sh login OR [opencga][demo@project:study]<demo/>login
        if (args.length == 2 && "login".equals(args[1])) {
            return forceLogin(args);
        }

        //CASES
        //case opencga.sh login --host ...... OR [opencga][demo@project:study]<demo/>login --host ......
        //case opencga.sh login user1 [.....] OR [opencga][demo@project:study]<demo/>login user1 [.....]
        if (args.length > 2 && "login".equals(args[1])) {
            if (args[2].equals("--host")) {
                return forceLogin(args);
            } else {
                String user = args[2];
                args = ArrayUtils.remove(args, 2);
                return loginUser(args, user);
            }
        }

        return args;
    }


}
