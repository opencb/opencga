package org.opencb.opencga.app.cli.main.parent;

import com.beust.jcommander.JCommander;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.CliOptionsParser;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.*;

public class ParentCliOptionsParser extends CliOptionsParser {

    protected final GeneralCliOptions.CommonCommandOptions commonCommandOptions;

    public ParentCliOptionsParser() {
        commonCommandOptions = new GeneralCliOptions.CommonCommandOptions();
    }


    @Override
    public boolean isHelp() {
        String parsedCommand = jCommander.getParsedCommand();
        if (parsedCommand != null) {
            JCommander jCommander2 = jCommander.getCommands().get(parsedCommand);
            List<Object> objects = jCommander2.getObjects();
            if (!objects.isEmpty() && objects.get(0) instanceof AdminCliOptionsParser.AdminCommonCommandOptions) {
                return ((AdminCliOptionsParser.AdminCommonCommandOptions) objects.get(0)).commonOptions.help;
            }
        }
        return commonCommandOptions.help;
    }


    public boolean isValid(String parsedCommand) {
        if (StringUtils.isEmpty(parsedCommand)) {
            // 1. Check if a command has been provided
            org.opencb.opencga.app.cli.main.utils.CommandLineUtils.debug("IS EMPTY COMMAND " + parsedCommand);
            return false;
        } else {
            // 2. Check if a subcommand has been provided
            String parsedSubCommand = getSubCommand();
            org.opencb.opencga.app.cli.main.utils.CommandLineUtils.debug("PARSED SUBCOMMAND " + parsedCommand);
            return !StringUtils.isEmpty(parsedSubCommand);
        }
    }

    @Override
    public void printUsage() {
        String parsedCommand = getCommand();
        if (parsedCommand.isEmpty()) {
            System.err.println();
            System.err.println("Program:     OpenCGA (OpenCB)");
            System.err.println("Version:     " + GitRepositoryState.get().getBuildVersion());
            System.err.println("Git commit:  " + GitRepositoryState.get().getCommitId());
            System.err.println("Description: Big Data platform for processing and analysing NGS data");
            System.err.println();
            System.err.println("Usage:       opencga.sh [-h|--help] [--version] <command> [options]");
            System.err.println();
            printMainUsage();
            System.err.println();
        } else {
            String parsedSubCommand = getSubCommand();
            if (parsedSubCommand.isEmpty()) {
                System.err.println();
                System.err.println("Usage:   opencga.sh " + parsedCommand + " <subcommand> [options]");
                System.err.println();
                System.err.println("Subcommands:");
                printCommands(jCommander.getCommands().get(parsedCommand));
                System.err.println();
            } else {
                System.err.println();
                System.err.println("Usage:   opencga.sh " + parsedCommand + " " + parsedSubCommand + " [options]");
                System.err.println();
                System.err.println("Options:");
                CommandLineUtils.printCommandUsage(jCommander.getCommands().get(parsedCommand).getCommands().get(parsedSubCommand));
                System.err.println();
            }
        }
    }

    @Override
    protected void printMainUsage() {
        Set<String> analysisCommands = new HashSet<>(Arrays.asList("alignments", "variant", "clinical"));
        Set<String> operationsCommands = new HashSet<>(Collections.singletonList("operations"));
        Map<String, String> opencgaCommands = getOpencgaCommands();
        System.err.println("Opencga commands:");
        for (Map.Entry entry : opencgaCommands.entrySet()) {
            System.err.printf("%30s  %s\n", entry.getKey(), entry.getValue());
        }
        System.err.println("Catalog commands:");
        for (String command : jCommander.getCommands().keySet()) {
            if (!analysisCommands.contains(command) && !operationsCommands.contains(command)) {
                System.err.printf("%30s  %s\n", command, jCommander.getCommandDescription(command));
            }
        }

        System.err.println();
        System.err.println("Analysis commands:");
        for (String command : jCommander.getCommands().keySet()) {
            if (analysisCommands.contains(command)) {
                System.err.printf("%30s  %s\n", command, jCommander.getCommandDescription(command));
            }
        }

        System.err.println();
        System.err.println("Operation commands:");
        for (String command : jCommander.getCommands().keySet()) {
            if (operationsCommands.contains(command)) {
                System.err.printf("%30s  %s\n", command, jCommander.getCommandDescription(command));
            }
        }
    }

    private Map<String, String> getOpencgaCommands() {
        Map<String, String> h = new HashMap<>();
        h.put("shell", "Interactive mode opencga shell");
        h.put("use study <name>", "(Only in interactive mode) Sets the study to be used in the following commands");
        h.put("use host <name>", "Sets the host(server) to be used in the following commands");
        h.put("login [user]", "Authenticates new user in the system");
        h.put("logout", "Logouts the current user from the system");
        h.put("exit", "Closes the opencga shell");
        return h;
    }
}
