package org.opencb.opencga.test.utils;

import org.opencb.commons.utils.PrintUtils;

import java.util.logging.Level;

public class OpencgaLogger {


    private static Level openCGALogLevel = Level.OFF;

    public static void printLog(String message, Throwable t) {

        printLog(message, openCGALogLevel);
        if (!openCGALogLevel.equals(Level.SEVERE) && !openCGALogLevel.equals(Level.OFF)) {
            t.printStackTrace();
        }
    }


    public static void printLog(String message, Level logLevel) {
        if (checkLogLevel(logLevel)) {
            if (logLevel.equals(Level.FINE)) {
                PrintUtils.printDebug(message);
            } else if (logLevel.equals(Level.INFO)) {
                PrintUtils.printInfo(message);
            } else if (logLevel.equals(Level.WARNING)) {
                PrintUtils.printWarn(message);
            } else if (logLevel.equals(Level.SEVERE)) {
                PrintUtils.printError(message);
            }
        }
    }

    private static boolean checkLogLevel(Level level) {
        if (Level.FINE.equals(level)) {
            return true;
        } else if (Level.INFO.equals(level) && (openCGALogLevel.equals(Level.INFO)
                || openCGALogLevel.equals(Level.WARNING)
                || openCGALogLevel.equals(Level.SEVERE))) {
            return true;
        } else if (Level.WARNING.equals(level) && (openCGALogLevel.equals(Level.SEVERE)
                || openCGALogLevel.equals(Level.WARNING))) {
            return true;
        } else return Level.SEVERE.equals(level) && (openCGALogLevel.equals(Level.SEVERE));

    }

    public static void setLogLevel(String level) {
        try {
            openCGALogLevel = getNormalizedLogLevel(level);
            if (openCGALogLevel != Level.OFF) {
                printLog("Log level " + openCGALogLevel.getName(), openCGALogLevel);
            }
        } catch (Exception e) {
            openCGALogLevel = Level.SEVERE;
            PrintUtils.printError("Invalid log level. Valid values are INFO, WARN, DEBUG, ERROR", e);
            System.exit(-1);
        }
    }


    private static Level getNormalizedLogLevel(String level) {
        switch (level) {
            case "debug":
            case "fine":
                return Level.FINE;
            case "info":
                return Level.INFO;
            case "warning":
            case "warn":
                return Level.WARNING;
            case "error":
            case "severe":
                return Level.SEVERE;
            default:
                return Level.OFF;
        }
    }
}
