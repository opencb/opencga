package org.opencb.opencga.server.generator.config;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CategoryConfig {

    private String name;
    private boolean ignore;
    private String key;
    private boolean executorExtended;
    private String executorExtendedClassName;
    private boolean optionExtended;
    private String optionExtendedClassName;
    private List<String> addedMethods;
    private List<Shortcut> shortcuts;
    private List<Command> commands;
    private String commandName;
    private boolean analysis;
    private boolean operations;

    public CategoryConfig() {

    }

    @Deprecated
    public CategoryConfig(String name, boolean ignore, String key, boolean executorExtended, boolean optionExtended,
                          List<String> addedMethods, List<Shortcut> shortcuts, List<Command> commands, String commandName,
                          boolean analysis, boolean operations) {
        this.name = name;
        this.ignore = ignore;
        this.key = key;
        this.executorExtended = executorExtended;
        this.optionExtended = optionExtended;
        this.addedMethods = addedMethods;
        this.shortcuts = shortcuts;
        this.commands = commands;
        this.commandName = commandName;
        this.analysis = analysis;
        this.operations = operations;
    }

    public CategoryConfig(String name, boolean ignore, String key, boolean executorExtended,
                          String executorExtendedClassName, boolean optionExtended, String optionExtendedClassName,
                          List<String> addedMethods, List<Shortcut> shortcuts, List<Command> commands,
                          String commandName, boolean analysis, boolean operations) {
        this.name = name;
        this.ignore = ignore;
        this.key = key;
        this.executorExtended = executorExtended;
        this.executorExtendedClassName = executorExtendedClassName;
        this.optionExtended = optionExtended;
        this.optionExtendedClassName = optionExtendedClassName;
        this.addedMethods = addedMethods;
        this.shortcuts = shortcuts;
        this.commands = commands;
        this.commandName = commandName;
        this.analysis = analysis;
        this.operations = operations;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CategoryConfig{");
        sb.append("name='").append(name).append('\'');
        sb.append(", ignore=").append(ignore);
        sb.append(", key='").append(key).append('\'');
        sb.append(", executorExtended=").append(executorExtended);
        sb.append(", optionExtended=").append(optionExtended);
        sb.append(", addedMethods=").append(addedMethods);
        sb.append(", shortcuts=").append(shortcuts);
        sb.append(", commands=").append(commands);
        sb.append(", commandName='").append(commandName).append('\'');
        sb.append(", analysis=").append(analysis);
        sb.append(", operations=").append(operations);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getAddedMethods() {
        return addedMethods;
    }

    public CategoryConfig setAddedMethods(List<String> addedMethods) {
        this.addedMethods = addedMethods;
        return this;
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

    public boolean isExecutorExtended() {
        return executorExtended;
    }

    public CategoryConfig setExecutorExtended(boolean executorExtended) {
        this.executorExtended = executorExtended;
        return this;
    }

    public boolean isOptionExtended() {
        return optionExtended;
    }

    public CategoryConfig setOptionExtended(boolean optionExtended) {
        this.optionExtended = optionExtended;
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

    public Command getCommandOrFail(String commandName) {
        Command command = getCommand(commandName);
        if (command == null) {
            List<String> availableCommands;
            if (commands == null) {
                availableCommands = Collections.emptyList();
            } else {
                availableCommands = commands.stream().map(Command::getName).collect(Collectors.toList());
            }
            throw new IllegalArgumentException("Command '" + commandName + "' not found. Given category '" + name + "'" +
                    ", available sub-commands are: " + availableCommands + ".");
        }
        return command;
    }

    public Command getCommand(String commandName) {
        if (commands != null) {
            for (Command cmd : commands) {
                if (cmd.getName().equals(commandName)) {
                    return cmd;
                }
            }
        }
        return null;
    }

    public boolean isExtendedOptionCommand(String commandName) {
        if (commands != null) {
            for (Command cmd : commands) {
                if (cmd.getName().equals(commandName) && cmd.isOptionExtended()) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isAvailableSubCommand(String sbname, String commandName) {
        if (commands != null) {
            for (Command cmd : commands) {
                if (cmd.getSubcommands() != null) {
                    for (Subcommand subcmd : cmd.getSubcommands()) {
                        if (subcmd.getName().equals(sbname) && subcmd.isIgnore()) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    public boolean isExecutorExtendedCommand(String commandName) {

        boolean res = false;
        if (commands != null) {
            for (Command cmd : commands) {
                if (cmd.getName().equals(commandName)) {
                    res = cmd.isExecutorExtended();
                }
            }
        }

        return res;
    }

    public String getExecutorExtendedClassName() {
        return executorExtendedClassName;
    }

    public CategoryConfig setExecutorExtendedClassName(String executorExtendedClassName) {
        this.executorExtendedClassName = executorExtendedClassName;
        return this;
    }

    public String getOptionExtendedClassName() {
        return optionExtendedClassName;
    }

    public CategoryConfig setOptionExtendedClassName(String optionExtendedClassName) {
        this.optionExtendedClassName = optionExtendedClassName;
        return this;
    }
}
