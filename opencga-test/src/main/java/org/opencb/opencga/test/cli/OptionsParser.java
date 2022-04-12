package org.opencb.opencga.test.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
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

        PrintUtils.println(getHelpVersionString());
    }

    public static void printUsage() {

        PrintUtils.println();
        PrintUtils.println(getHelpVersionString());
        PrintUtils.println();
        PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("Usage:", "       opencga-test.sh [--config configFile.yml] [--help] [--version]"));
        PrintUtils.println();

        JCommander currentCommand = jcommander.getCommands().get(jcommander.getParsedCommand());
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
        int padding = 10 + commander.getCommands().keySet().stream().mapToInt(String::length).max().orElse(0);

        List<String> cmds = commander.getCommands().keySet().stream().sorted().collect(Collectors.toList());
        for (String key : cmds) {
            PrintUtils.printCommandHelpFormattedString(padding, key, commander.getCommandDescription(key));
        }

        List<ParameterDescription> parameters = commander.getParameters();
        Map<String, String> parameterMap = new HashMap<>();
        for (ParameterDescription parameter : parameters) {
            parameterMap.put(parameter.getNames(), parameter.getDescription());
        }
        PrintUtils.println();
        PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("Parameters:", ""));
        PrintUtils.println();
        for (String key : parameterMap.keySet()) {
            PrintUtils.printCommandHelpFormattedString(padding, key, parameterMap.get(key));
        }
        PrintUtils.printAsTable(parameterMap, PrintUtils.Color.YELLOW, PrintUtils.Color.GREEN, 4);
    }


    public static String getHelpVersionString() {
        String res = PrintUtils.getHelpVersionFormattedString("OpenCGA Test version: ", "\t" + GitRepositoryState.get().getBuildVersion() + "\n");
        res += PrintUtils.getHelpVersionFormattedString("Git version:", "\t\t" + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId() + "\n");
        res += PrintUtils.getHelpVersionFormattedString("Program:", "\t\tOpenCGA-test (OpenCB)" + "\n");
        res += PrintUtils.getHelpVersionFormattedString("Description: ", "\t\tData generation application for the openCGA platform" + "\n");
        return res;
    }


}
