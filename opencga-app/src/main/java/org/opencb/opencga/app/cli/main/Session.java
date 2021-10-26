package org.opencb.opencga.app.cli.main;

public class Session {

    private boolean shell = false;

    public boolean isShell() {
        return shell;
    }

    public Session setShell(boolean shell) {
        this.shell = shell;
        return this;
    }
}
