package org.opencb.opencga.app.cli.main;

import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.core.common.GitRepositoryState;


public class CommandLineUtils {

    private static final boolean forceDebug = false;

    public static String getVersionString() {
        String res = PrintUtils.getKeyValueAsFormattedString("\tOpenCGA CLI version: ", "\t" + GitRepositoryState.get().getBuildVersion() + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tGit version:", "\t\t" + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId() + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tProgram:", "\t\tOpenCGA (OpenCB)" + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tDescription: ", "\t\tBig Data platform for processing and analysing NGS data" + "\n");
        return res;
    }


    public static void printError(String message, Exception e) {
        if (OpencgaMain.isDebug() || forceDebug) {
            PrintUtils.printError(message, e);
        } else {
            PrintUtils.printError(message);
        }
    }

    public static void printDebug(String s) {
        if (OpencgaMain.isDebug() || forceDebug) {
            PrintUtils.printDebug(s);
        }
    }

    public static boolean isValidUser(String user) {
        return user.matches("^[A-Za-z][A-Za-z0-9_]{2,29}$");
    }
}
