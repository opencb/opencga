package org.opencb.opencga.server.json.config;

import java.util.List;

public class Command {

    private boolean ignore;
    private String name;
    private boolean extended;
    private List<Subcommand> subcommands;

    public Command(boolean ignore, String name, boolean extended, List<Subcommand> subcommands) {
        this.ignore = ignore;
        this.name = name;
        this.extended = extended;
        this.subcommands = subcommands;
    }

    public Command() {
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

    public boolean isExtended() {
        return extended;
    }

    public Command setExtended(boolean extended) {
        this.extended = extended;
        return this;
    }

    public List<Subcommand> getSubcommands() {
        return subcommands;
    }

    public Command setSubcommands(List<Subcommand> subcommands) {
        this.subcommands = subcommands;
        return this;
    }
}
