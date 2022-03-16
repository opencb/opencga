package org.opencb.opencga.app.cli.main.utils;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaMain;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.List;
import java.util.logging.Level;

import static org.opencb.commons.utils.PrintUtils.*;


public class CommandLineUtils {


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

    private static void printLevel(String message, Level level) {
        if (checkLevel(level)) {
            printLog(message);
        }
    }

    public static boolean isNotHelpCommand(String[] args) {
        return !ArrayUtils.contains(args, "--help") && !ArrayUtils.contains(args, "-h");
    }

    private static void printLevel(String message, Exception e, Level level) {
        if (checkLevel(level)) {
            printLog(message, e);
        }
    }

    public static void printLog(String message, Exception e) {

        if (OpencgaMain.getLogLevel().equals(Level.FINE)) {
            PrintUtils.printDebug(message);
        } else if (OpencgaMain.getLogLevel().equals(Level.INFO)) {
            PrintUtils.printInfo(message);
        } else if (OpencgaMain.getLogLevel().equals(Level.WARNING)) {
            printWarn(message);
        } else if (OpencgaMain.getLogLevel().equals(Level.SEVERE)) {
            PrintUtils.printError(message, e);
        }
    }

    public static void printLog(String s) {
        printLog(s, null);
    }

    public static boolean isValidUser(String user) {
        return user.matches("^[A-Za-z][A-Za-z0-9_\\-ñÑ]{2,29}$");
    }

    private static boolean checkLevel(Level level) {
        if (Level.FINE.equals(level)) {
            return true;
        } else if (Level.INFO.equals(level) && (OpencgaMain.getLogLevel().equals(Level.INFO)
                || OpencgaMain.getLogLevel().equals(Level.WARNING)
                || OpencgaMain.getLogLevel().equals(Level.SEVERE))) {
            return true;
        } else if (Level.WARNING.equals(level) && (OpencgaMain.getLogLevel().equals(Level.SEVERE)
                || OpencgaMain.getLogLevel().equals(Level.WARNING))) {
            return true;
        } else return Level.SEVERE.equals(level) && (OpencgaMain.getLogLevel().equals(Level.SEVERE));

    }

    public static void error(Exception e) {
        printError(e.getMessage());
        CommandLineUtils.printLevel(e.getMessage(), e, Level.SEVERE);
    }

    public static void error(String message, Exception e) {
        printError(message);
        CommandLineUtils.printLevel(e.getMessage(), e, Level.SEVERE);
    }

    public static void debug(String s) {
        CommandLineUtils.printLevel(s, Level.FINE);
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
        String sArgs = ArrayUtils.toString(args);
        sArgs = sArgs.replaceAll("\\{", "");
        sArgs = sArgs.replaceAll("}", "");
        sArgs = sArgs.replaceAll(",", " ");
        return sArgs;
    }
}
