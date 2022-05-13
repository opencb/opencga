package org.opencb.opencga.test.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.test.cli.executors.DatasetCommandExecutor;
import org.opencb.opencga.test.cli.options.DatasetCommandOptions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class OptionsParser {

    @Parameter(names = "--help", description = "Show help.")
    public static boolean help = false;

    @Parameter(names = "--version", description = "Show current version information.")
    public static boolean version = false;

    private static JCommander jcommander;

    static {
        OptionsParser parser = new OptionsParser();
        jcommander = JCommander.newBuilder().addObject(parser).build();
        loadCommands();
    }

    public static void parseArgs(String[] args) {

        jcommander.parse(args);

        if (help) {
            printUsage();
        } else if (version) {
            printVersion();
        } else {
            if (jcommander.getParsedCommand() != null) {
                execute(jcommander.getParsedCommand());
            } else {
                PrintUtils.printWarn("No valid command found.");
            }
        }
    }

    private static void execute(String parsedCommand) {

        switch (parsedCommand) {
            case "dataset":
                new DatasetCommandExecutor().execute();
                break;
            default:
                break;
        }
    }

    private static void loadCommands() {
        DatasetCommandOptions dataset = new DatasetCommandOptions();
        jcommander.addCommand("dataset", dataset);
    }

    public static void printVersion() {

        Map<String, String> versionMap = getVersionMap();
        int padding = 20 + versionMap.keySet().stream().mapToInt(String::length).max().orElse(0);
        for (String key : versionMap.keySet()) {
            PrintUtils.printCommandHelpFormattedString(padding, key, versionMap.get(key));
        }
        PrintUtils.println();
    }

    private static Map<String, String> getVersionMap() {
        Map<String, String> versionMap = new HashMap<>();
        versionMap.put("OpenCGA Test version:", GitRepositoryState.get().getBuildVersion());
        versionMap.put("Git version:", "" + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId());
        versionMap.put("Program:", "OpenCGA-test (OpenCB)");
        versionMap.put("Description:", "Data generation application for the openCGA platform");
        return versionMap;
    }

    public static void printUsage() {
        JCommander currentCommand = jcommander.getCommands().get(jcommander.getParsedCommand());
        printVersion();
        PrintUtils.println();
        if (currentCommand != null) {
            PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("    Usage:", "   opencga-test.sh " + currentCommand.getProgramName() + " [options] [--help] [--version]"));
        } else {
            PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("    Usage:", "   opencga-test.sh [command] [options] [--help] [--version]"));
        }
        PrintUtils.println();

        if (currentCommand == null) {
            currentCommand = jcommander;
        }
        printCommandsAndParameters(currentCommand);

      /*  List<ParameterDescription> parameters = currentCommand.getParameters();
        Map<String, String> parameterMap = new HashMap<>();
        for (ParameterDescription parameter : parameters) {
            parameterMap.put(parameter.getNames(), parameter.getDescription());
        }*/

        //  PrintUtils.printAsTable(parameterMap, PrintUtils.Color.YELLOW, PrintUtils.Color.GREEN, 4);
        PrintUtils.println();

    }


    private static void printCommandsAndParameters(JCommander commander) {
        // Calculate the padding needed and add 10 extra spaces to get some left indentation
        int padding = 20 + commander.getCommands().keySet().stream().mapToInt(String::length).max().orElse(0);

        List<String> cmds = commander.getCommands().keySet().stream().sorted().collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(cmds)) {
            PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("    Commands:", ""));
            for (String key : cmds) {
                PrintUtils.printCommandHelpFormattedString(padding, key, commander.getCommandDescription(key));
            }
            PrintUtils.println();
        }
        List<ParameterDescription> parameters = commander.getParameters();
        if (CollectionUtils.isNotEmpty(parameters)) {
            PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("    Options:", ""));
            for (ParameterDescription parameter : parameters) {
                PrintUtils.printCommandHelpFormattedString(padding, parameter.getNames(), parameter.getDescription());
            }
            PrintUtils.println();
        }
    }
}
