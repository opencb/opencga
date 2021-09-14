package org.opencb.opencga.server.json.config;

public class Subcommand {

    private boolean ignore;
    private String name;

    public Subcommand(boolean ignore, String name) {
        this.ignore = ignore;
        this.name = name;
    }

    public Subcommand() {
    }

    public boolean isIgnore() {
        return ignore;
    }

    public Subcommand setIgnore(boolean ignore) {
        this.ignore = ignore;
        return this;
    }

    public String getName() {
        return name;
    }

    public Subcommand setName(String name) {
        this.name = name;
        return this;
    }
}


