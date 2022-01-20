package org.opencb.opencga.app.cli.main.utils;

import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.main.OpencgaMain;
import org.opencb.opencga.app.cli.session.LogLevel;
import org.opencb.opencga.core.common.GitRepositoryState;

import static org.opencb.commons.utils.PrintUtils.printError;


public class CommandLineUtils {


    public static String getVersionString() {
        String res = PrintUtils.getKeyValueAsFormattedString("\tOpenCGA CLI version: ", "\t" + GitRepositoryState.get().getBuildVersion() + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tGit version:", "\t\t" + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId() + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tProgram:", "\t\tOpenCGA (OpenCB)" + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tDescription: ", "\t\tBig Data platform for processing and analysing NGS data" + "\n");
        return res;
    }

    private static void printLogLevel(String message, LogLevel level) {
        if (checkLogLevel(level)) {
            printLog(message);
        }
    }


    private static void printLogLevel(String message, Exception e, LogLevel level) {
        if (checkLogLevel(level)) {
            printLog(message, e);
        }
    }

    public static void printLog(String message, Exception e) {

        switch (OpencgaMain.getLogLevel()) {
            case DEBUG:
                PrintUtils.printDebug(message);
                break;
            case INFO:
                PrintUtils.printInfo(message);
                break;
            case WARN:
                PrintUtils.printWarn(message);
                break;
            case ERROR:
                PrintUtils.printError(message, e);
                break;
        }
    }

    public static void printLog(String s) {
        printLog(s, null);
    }


    public static void printMessage(String s) {
        PrintUtils.printInfo(s);
    }

    public static boolean isValidUser(String user) {
        return user.matches("^[A-Za-z][A-Za-z0-9_]{2,29}$");
    }

    private static boolean checkLogLevel(LogLevel level) {
        switch (level) {
            case DEBUG:
                return true;
            case INFO:
                if (OpencgaMain.getLogLevel().equals(LogLevel.INFO)
                        || OpencgaMain.getLogLevel().equals(LogLevel.WARN)
                        || OpencgaMain.getLogLevel().equals(LogLevel.ERROR)) {
                    return true;
                }
                break;
            case WARN:
                if (OpencgaMain.getLogLevel().equals(LogLevel.WARN)
                        || OpencgaMain.getLogLevel().equals(LogLevel.ERROR)) {
                    return true;
                }
                break;
            case ERROR:
                if (OpencgaMain.getLogLevel().equals(LogLevel.ERROR)) {
                    return true;
                }
                break;
        }
        return false;
    }

    public static void error(Exception e) {
        printError(e.getMessage());
        CommandLineUtils.printLogLevel(e.getMessage(), e, LogLevel.ERROR);
    }

    public static void error(String message, Exception e) {
        printError(message);
        CommandLineUtils.printLogLevel(e.getMessage(), e, LogLevel.ERROR);
    }

    public static void debug(String s) {
        CommandLineUtils.printLogLevel(s, LogLevel.DEBUG);
    }
}
