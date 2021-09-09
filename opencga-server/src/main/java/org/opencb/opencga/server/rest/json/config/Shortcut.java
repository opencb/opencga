package org.opencb.opencga.server.rest.json.config;

public class Shortcut {

    private String command;
    private char shortcut;

    public Shortcut() {
    }

    public Shortcut(String command, char shortcut) {
        this.command = command;
        this.shortcut = shortcut;
    }

    public String getCommand() {
        return command;
    }

    public Shortcut setCommand(String command) {
        this.command = command;
        return this;
    }

    public char getShortcut() {
        return shortcut;
    }

    public Shortcut setShortcut(char shortcut) {
        this.shortcut = shortcut;
        return this;
    }
}
