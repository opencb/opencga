package org.opencb.opencga.app.cli.main;

import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CommandLineUtils {

    public static List getListValues(String value) {
        String[] vec = value.split(",");
        for (int i = 0; i < vec.length; i++) {
            vec[i] = vec[i].trim();
        }
        return Arrays.asList(vec);
    }

    public static String getVersionString() {
        String res = PrintUtils.getKeyValueAsFormattedString("\tOpenCGA CLI version: ", "\t" + GitRepositoryState.get().getBuildVersion() + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tGit version:", "\t\t" + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId() + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tProgram:", "\t\tOpenCGA (OpenCB)" + "\n");
        res += PrintUtils.getKeyValueAsFormattedString("\tDescription: ", "\t\tBig Data platform for processing and analysing NGS data" + "\n");
        return res;
    }

    public static String getAsCommaSeparatedString(String[] args) {
        return Stream.of(args).collect(Collectors.joining(","));
    }

    public static String getAsSpaceSeparatedString(String[] args) {
        return Stream.of(args).collect(Collectors.joining(" "));
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
            PrintUtils.printDebugMessage(s);
        }
    }


}
