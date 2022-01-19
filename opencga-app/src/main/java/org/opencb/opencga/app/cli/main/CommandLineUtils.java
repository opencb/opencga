package org.opencb.opencga.app.cli.main;

import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.core.common.GitRepositoryState;


public class CommandLineUtils {

  
    public static String getVersionString() {
        String res = PrintUtils.getKeyValueAsFormattedString("\tOpenCGA CLI version: ", "\t" + GitRepositoryState.get().getBuildVersion() + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tGit version:", "\t\t" + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId() + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tProgram:", "\t\tOpenCGA (OpenCB)" + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tDescription: ", "\t\tBig Data platform for processing and analysing NGS data" + "\n");
        return res;
    }


    public static void printLog(String message, Exception e) {

        switch (OpencgaMain.getLogLevel()) {
            case "info":
                PrintUtils.printInfo(message);
                break;
            case "warn":
                PrintUtils.printWarn(message);
                break;
            case "debug":
                PrintUtils.printDebug(message);
                break;
            case "error":
                PrintUtils.printError(message, e);
                break;
        }
    }

    public static void printLog(String s) {
        printLog(s, null);
    }

    public static boolean isValidUser(String user) {
        return user.matches("^[A-Za-z][A-Za-z0-9_]{2,29}$");
    }
}
