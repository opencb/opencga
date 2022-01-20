package org.opencb.opencga.app.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.main.utils.LoginUtils;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.Map;

import static org.opencb.commons.utils.PrintUtils.println;

/**
 * Created on 08/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class CliOptionsParser {

    protected final JCommander jCommander;
    protected final GeneralCliOptions.GeneralOptions generalOptions;

    public CliOptionsParser() {
        generalOptions = new GeneralCliOptions.GeneralOptions();
        this.jCommander = new JCommander(generalOptions);
    }

    public static String getSubCommand(JCommander jCommander) {
        String parsedCommand = jCommander.getParsedCommand();
        if (jCommander.getCommands().containsKey(parsedCommand)) {
            String subCommand = jCommander.getCommands().get(parsedCommand).getParsedCommand();
            return subCommand != null ? subCommand : "";
        } else {
            return null;
        }
    }

    public String[] parse(String[] args) throws ParameterException {

        //Process the shortcuts login, help, version, logout...
        args = processShortCuts(args);
        if (!ArrayUtils.isEmpty(args)) {
            jCommander.parse(args);
        }
        return args;
    }

    public String[] processShortCuts(String[] args) {
        switch (args[0]) {
            case "login":
                return LoginUtils.parseLoginCommand(args);
            case "--help":
            case "help":
            case "-h":
            case "?":
                printUsage();
                return null;
            case "--version":
            case "version":
                println(CommandLineUtils.getVersionString());
                return null;
            case "--build-version":
            case "build-version":
                println(GitRepositoryState.get().getBuildVersion());
                return null;
            case "logout":
                return ArrayUtils.addAll(new String[]{"users"}, args);
            default:
                return args;
        }
    }

    public String getCommand() {
        return (jCommander.getParsedCommand() != null) ? jCommander.getParsedCommand() : "";
    }

    public String getSubCommand() {
        return getSubCommand(jCommander);
    }

    public abstract boolean isHelp();

    public abstract void printUsage();

    protected void printMainUsage() {
        printCommands(jCommander);
    }

    protected void printCommands(JCommander commander) {
        int pad = commander.getCommands().keySet().stream().mapToInt(String::length).max().orElse(0);
        // Set padding between 14 and 40
        pad = Math.max(14, pad);
        pad = Math.min(40, pad);
        for (Map.Entry<String, JCommander> entry : commander.getCommands().entrySet()) {
            System.err.printf("%" + pad + "s  %s\n", entry.getKey(), commander.getCommandDescription(entry.getKey()));
        }
    }

    public JCommander getJCommander() {
        return jCommander;
    }

    public GeneralCliOptions.GeneralOptions getGeneralOptions() {
        return generalOptions;
    }
}

