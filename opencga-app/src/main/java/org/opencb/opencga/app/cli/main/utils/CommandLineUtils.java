package org.opencb.opencga.app.cli.main.utils;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaMain;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.opencb.commons.utils.PrintUtils.*;


public class CommandLineUtils {

    private static final Logger logger = LoggerFactory.getLogger(CommandLineUtils.class);

    public static String getVersionString() {
        String res = PrintUtils.getKeyValueAsFormattedString("\tOpenCGA CLI version: ", "\t" + GitRepositoryState.get().getBuildVersion() + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tGit version:", "\t\t" + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId() + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tProgram:", "\t\tOpenCGA (OpenCB)" + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tDescription: ", "\t\tBig Data platform for processing and analysing NGS data" + "\n");
        return res;
    }

    public static String getHelpVersionString() {
        String res = PrintUtils.getHelpVersionFormattedString("OpenCGA CLI version: ", "\t" + GitRepositoryState.get().getBuildVersion() + "\n");
        res += PrintUtils.getHelpVersionFormattedString("Git version:", "\t\t" + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId() + "\n");
        res += PrintUtils.getHelpVersionFormattedString("Program:", "\t\tOpenCGA (OpenCB)" + "\n");
        res += PrintUtils.getHelpVersionFormattedString("Description: ", "\t\tBig Data platform for processing and analysing NGS data" + "\n");
        return res;
    }

    public static boolean isNotHelpCommand(String[] args) {
        return !isHelpCommand(args);
    }

    public static boolean isHelpCommand(String[] args) {
        return ArrayUtils.contains(args, "--help") || ArrayUtils.contains(args, "-h");
    }

    public static boolean isValidUser(String user) {
        return user.matches("^[A-Za-z][A-Za-z0-9_\\-ñÑ]{2,29}$");
    }

    public static void error(String message) {
        printError(message);
    }

    public static void error(Exception e) {
        printError(e.getMessage());
    }

    public static void error(String message, Exception e) {
        if (e == null) {
            printError(message);
        } else {
            printError(message + " : " + e.getMessage());
        }
    }

    public static String[] processShortCuts(String[] args) {
        OpencgaCliOptionsParser cliOptionsParser = new OpencgaCliOptionsParser();
        switch (getShortcut(args)) {
            case "login":
                return LoginUtils.parseLoginCommand(args);
            case "--help":
            case "help":
            case "-h":
            case "?":
                if (ArrayUtils.contains(args, "help")) {
                    for (int i = 0; i < args.length; i++) {
                        if (args[i].equals("help") || args[i].equals("?") || args[i].equals("-h")) {
                            args[i] = "--help";
                        }
                    }
                }
                try {
                    cliOptionsParser.printUsage(args);
                } catch (Exception e) {
                    // malformed command
                    return args;
                }
                break;
            case "--version":
            case "version":
                println(CommandLineUtils.getVersionString());
                break;
            case "--build-version":
            case "build-version":
                println(GitRepositoryState.get().getBuildVersion());
                break;
            case "logout":
                return ArrayUtils.addAll(new String[]{"users"}, args);
            case "list":
                if (OpencgaMain.isShellMode()) {
                    if (args.length > 1 && args[1].equals("studies")) {
                        List<String> studies = OpencgaMain.getShell().getSessionManager().getSession().getStudies();
                        for (String study : studies) {
                            printGreen(study);
                        }
                    } else {
                        printWarn("Opencga version " + GitRepositoryState.get().getBuildVersion() + " can only list studies");
                    }
                } else {
                    printWarn("List studies is only available in Shell mode");
                }
                break;
            default:
                return args;
        }
        return null;
    }

    public static String getShortcut(String[] args) {
        if (ArrayUtils.contains(args, "--help")
                || ArrayUtils.contains(args, "-h")
                || ArrayUtils.contains(args, "?")
                || ArrayUtils.contains(args, "help")) {
            return "--help";
        }
        return args[0];
    }

    public static void printArgs(String[] args) {
        PrintUtils.println(argsToString(args));
    }

    public static String argsToString(String[] args) {

        String[] res = Arrays.copyOf(args, args.length);
        if (ArrayUtils.contains(res, "--password") && (ArrayUtils.indexOf(res, "--password") + 1) < res.length) {
            res[(ArrayUtils.indexOf(res, "--password") + 1)] = "********";
        }
        return String.join(" ", res);
    }
}
