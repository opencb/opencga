package org.opencb.opencga.app.cli.main;

import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.core.common.GitRepositoryState;


public class CommandLineUtils {

    private static final boolean forceDebug = true;

    public static String getVersionString() {
        String res = PrintUtils.getKeyValueAsFormattedString("\tOpenCGA CLI version: ", "\t" + GitRepositoryState.get().getBuildVersion() + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tGit version:", "\t\t" + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId() + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tProgram:", "\t\tOpenCGA (OpenCB)" + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tDescription: ", "\t\tBig Data platform for processing and analysing NGS data" + "\n");
        return res;
    }

    @Deprecated
    public static String getAsSpaceSeparatedString(String[] args) {
        return String.join(" ", args);
    }

    public static void printError(String message, Exception e) {
        if (CliSessionManager.isDebug() || forceDebug) {
            PrintUtils.printError(message, e);
        } else {
            PrintUtils.printError(message);
        }
    }

    public static void printDebug(String s) {
        if (CliSessionManager.isDebug() || forceDebug) {
            PrintUtils.printDebug(s);
        }
    }


}
