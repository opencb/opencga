package org.opencb.opencga.app.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.opencb.commons.utils.PrintUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    public void parse(String[] args) throws ParameterException {
        jCommander.parse(args);
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

        List<String> cmds = new ArrayList(commander.getCommands().keySet());
        Collections.sort(cmds);
        for (String key : cmds) {
            PrintUtils.printCommandHelpFormattedString(key, commander.getCommandDescription(key));
        }
    }

    public JCommander getJCommander() {
        return jCommander;
    }

    public GeneralCliOptions.GeneralOptions getGeneralOptions() {
        return generalOptions;
    }
}

