package org.opencb.opencga.app.cli.main.parent;

import com.beust.jcommander.JCommander;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.CliOptionsParser;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaMain;

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

    public void printUsage(String[] args) {
        parse(args);
        printUsage();
    }

    @Override
    public void printUsage() {
        String parsedCommand = getCommand();
        if (parsedCommand.isEmpty()) {
            System.err.println();
            PrintUtils.println(org.opencb.opencga.app.cli.main.utils.CommandLineUtils.getHelpVersionString());
            System.err.println();
            if (OpencgaMain.isShellMode()) {
                PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("Usage:", "       " + getPrefix() + " <command> [options] [-h|--help] [--version]"));
            } else {
                PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("Usage:", "       " + getPrefix() + "[-h|--help] [--shell] [--host] [--version] <command> [options]"));
            }
            System.err.println();
            printMainUsage();
            System.err.println();
        } else {
            String parsedSubCommand = getSubCommand();
            if (parsedSubCommand.isEmpty()) {
                System.err.println();
                PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("Usage:", "   " + getPrefix() + parsedCommand + " <subcommand> [options]"));
                System.err.println();
                PrintUtils.println(PrintUtils.format("Subcommands:", PrintUtils.Color.GREEN));
                printCommands(jCommander.getCommands().get(parsedCommand));
                System.err.println();
            } else {
                System.err.println();
                PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("Usage:", "   " + getPrefix() + parsedCommand + " " + parsedSubCommand + " [options]"));
                System.err.println();
                PrintUtils.println(PrintUtils.format("Options:", PrintUtils.Color.GREEN));
                CommandLineUtils.printCommandUsage(jCommander.getCommands().get(parsedCommand).getCommands().get(parsedSubCommand));
                System.err.println();
            }
        }
    }

    private String getPrefix() {
        if (OpencgaMain.isShellMode()) {
            return "";
        }
        return "opencga.sh ";
    }

    @Override
    protected void printMainUsage() {
        Set<String> analysisCommands = new HashSet<>(Arrays.asList("alignments", "variant", "clinical"));
        Set<String> operationsCommands = new HashSet<>(Collections.singletonList("operations"));
        Map<String, String> opencgaCommands = getOpencgaCommands();

        String[] catalog = {"users", "projects", "studies", "files", "jobs", "individuals", "families", "panels", "samples", "cohorts", "meta"};
        PrintUtils.println(PrintUtils.format("Catalog commands:", PrintUtils.Color.GREEN));
        for (int i = 0; i < catalog.length; i++) {
            for (String command : jCommander.getCommands().keySet()) {
                if (command.equals(catalog[i])) {
                    PrintUtils.printCommandHelpFormattedString(command, jCommander.getCommandDescription(command));
                }
            }
        }
        System.err.println();
        PrintUtils.println(PrintUtils.format("Analysis commands:", PrintUtils.Color.GREEN));
        String[] analysis = {"alignments", "variant", "clinical"};
        for (int i = 0; i < analysis.length; i++) {
            for (String command : jCommander.getCommands().keySet()) {
                if (command.equals(analysis[i])) {
                    PrintUtils.printCommandHelpFormattedString(command, jCommander.getCommandDescription(command));
                }
            }
        }

        System.err.println();
        PrintUtils.println(PrintUtils.format("Operation commands:", PrintUtils.Color.GREEN));
        String[] operations = {"operations"};
        for (int i = 0; i < operations.length; i++) {
            for (String command : jCommander.getCommands().keySet()) {
                if (command.equals(operations[i])) {
                    PrintUtils.printCommandHelpFormattedString(command, jCommander.getCommandDescription(command));
                }
            }
        }

        System.err.println();
        if (!OpencgaMain.isShellMode()) {
            PrintUtils.println(PrintUtils.format("Opencga options:", PrintUtils.Color.GREEN));
        } else {
            PrintUtils.println(PrintUtils.format("Opencga commands:", PrintUtils.Color.GREEN));
        }
        for (Map.Entry entry : opencgaCommands.entrySet()) {
            PrintUtils.printCommandHelpFormattedString(entry.getKey().toString(), entry.getValue().toString());
        }
    }

    private Map<String, String> getOpencgaCommands() {
        Map<String, String> h = new HashMap<>();
        h.put("login [user]", "Authenticates new user in OpenCGA");
        h.put("logout", "Logouts the current user from OpenCGA");
        if (!OpencgaMain.isShellMode()) {
            h.put("--host", "Set the host server to query data");
            h.put("--shell", "Interactive mode opencga shell");
        } else {
            h.put("use study <name>", "Sets the study to be used in the following commands");
            h.put("list studies", "Print available studies for user");
            h.put("exit", "Closes the opencga shell");
        }
        return h;
    }
}
