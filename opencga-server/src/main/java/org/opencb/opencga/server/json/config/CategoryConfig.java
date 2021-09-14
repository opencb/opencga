package org.opencb.opencga.server.json.config;

import org.opencb.opencga.server.json.beans.Parameter;

import java.util.List;

public class CategoryConfig {

    private String name;
    private boolean ignore;
    private String key;
    private boolean extended;
    private List<Shortcut> shortcuts;
    private List<Command> commands;
    private String commandName;
    private boolean analysis;
    private boolean operations;

    public CategoryConfig() {
    }

    public CategoryConfig(String name, boolean ignore, String key, boolean extended, List<Shortcut> shortcuts, List<Command> commands) {
        this.name = name;
        this.ignore = ignore;
        this.key = key;
        this.extended = extended;
        this.shortcuts = shortcuts;
        this.commands = commands;
    }

    public String getName() {
        return name;
    }

    public CategoryConfig setName(String name) {
        this.name = name;
        return this;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public CategoryConfig setIgnore(boolean ignore) {
        this.ignore = ignore;
        return this;
    }

    public String getKey() {
        return key;
    }

    public CategoryConfig setKey(String key) {
        this.key = key;
        return this;
    }

    public boolean isExtended() {
        return extended;
    }

    public CategoryConfig setExtended(boolean extended) {
        this.extended = extended;
        return this;
    }

    public List<Shortcut> getShortcuts() {
        return shortcuts;
    }

    public CategoryConfig setShortcuts(List<Shortcut> shortcuts) {
        this.shortcuts = shortcuts;
        return this;
    }

    public List<Command> getCommands() {
        return commands;
    }

    public CategoryConfig setCommands(List<Command> commands) {
        this.commands = commands;
        return this;
    }

    public String getCommandName() {
        return commandName;
    }

    public CategoryConfig setCommandName(String commandName) {
        this.commandName = commandName;
        return this;
    }

    public boolean isAnalysis() {
        return analysis;
    }

    public CategoryConfig setAnalysis(boolean analysis) {
        this.analysis = analysis;
        return this;
    }

    public boolean isOperations() {
        return operations;
    }

    public CategoryConfig setOperations(boolean operations) {
        this.operations = operations;
        return this;
    }

    public boolean isAvailableCommand(String commandName) {
        if (commands != null) {
            for (Command cmd : commands) {
                if (cmd.getName().equals(commandName) && cmd.isIgnore()) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean isAvailableSubCommand(String name) {
        if (commands != null) {
            for (Command cmd : commands) {
                if (cmd.getSubcommands() != null) {
                    for (Subcommand subcmd : cmd.getSubcommands()) {
                        if (subcmd.getName().equals(commandName) && subcmd.isIgnore()) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    public String getStringShortcuts(Parameter parameter) {
        String res = "";
        if (getShortcuts() != null) {
            for (Shortcut sc : getShortcuts()) {
                if (parameter.getName().equals(sc.getName())) {
                    res += " -" + sc.getShortcut();
                }
            }
        }
        return res;
    }

    public boolean isExtendedCommand(String commandName) {
        if (commands != null) {
            for (Command cmd : commands) {
                if (cmd.getName().equals(commandName)) {
                    return cmd.isExtended();
                }
            }
        }
        return false;
    }
}
