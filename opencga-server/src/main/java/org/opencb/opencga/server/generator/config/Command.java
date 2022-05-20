package org.opencb.opencga.server.generator.config;

import java.util.List;

public class Command {

    private boolean ignore;
    private String name;
    private String rename;
    private boolean executorExtended;
    private boolean optionExtended;
    private List<Subcommand> subcommands;

    public Command() {
    }

    public Command(boolean ignore, String name, String rename, boolean executorExtended, boolean optionExtended,
                   List<Subcommand> subcommands) {
        this.ignore = ignore;
        this.name = name;
        this.rename = rename;
        this.executorExtended = executorExtended;
        this.optionExtended = optionExtended;
        this.subcommands = subcommands;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public Command setIgnore(boolean ignore) {
        this.ignore = ignore;
        return this;
    }

    public String getName() {
        return name;
    }

    public Command setName(String name) {
        this.name = name;
        return this;
    }

    public String getRename() {
        return rename;
    }

    public Command setRename(String rename) {
        this.rename = rename;
        return this;
    }

    public boolean isExecutorExtended() {
        return executorExtended;
    }

    public Command setExecutorExtended(boolean extended) {
        this.executorExtended = extended;
        return this;
    }

    public List<Subcommand> getSubcommands() {
        return subcommands;
    }

    public Command setSubcommands(List<Subcommand> subcommands) {
        this.subcommands = subcommands;
        return this;
    }

    public boolean isOptionExtended() {
        return optionExtended;
    }

    public Command setOptionExtended(boolean optionExtended) {
        this.optionExtended = optionExtended;
        return this;
    }
}
