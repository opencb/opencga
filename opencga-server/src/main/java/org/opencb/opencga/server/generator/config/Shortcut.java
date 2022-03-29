package org.opencb.opencga.server.generator.config;

public class Shortcut {

    private String name;
    private String shortcut;

    public Shortcut() {
    }

    public Shortcut(String name, String shortcut) {
        this.name = name;
        this.shortcut = shortcut;
    }

    public String getName() {
        return name;
    }

    public Shortcut setName(String name) {
        this.name = name;
        return this;
    }

    public String getShortcut() {
        return shortcut;
    }

    public Shortcut setShortcut(String shortcut) {
        this.shortcut = shortcut;
        return this;
    }
}
