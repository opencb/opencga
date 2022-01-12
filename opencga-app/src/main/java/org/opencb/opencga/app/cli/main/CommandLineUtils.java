package org.opencb.opencga.app.cli.main;

import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.ArrayList;
import java.util.List;

public class CommandLineUtils {

    public static List<String> splitWithTrim(String value) {
        return splitWithTrim(value, ",");
    }

    public static List<String> splitWithTrim(String value, String separator) {
        String[] splitFields = value.split(separator);
        List<String> result = new ArrayList<>(splitFields.length);
        for (String s : splitFields) {
            result.add(s.trim());
        }
        return result;
    }

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
        if (CliSessionManager.isDebug()) {
            PrintUtils.printError(message, e);
        } else {
            PrintUtils.printError(message);
        }
    }

    public static void printDebugMessage(String s) {
        if (CliSessionManager.isDebug()) {
            PrintUtils.printDebug(s);
        }
    }


}
