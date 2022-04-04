package org.opencb.opencga.test.plan;

public class CommandDataSet {

    private String commandLine;
    private String image;

    public CommandDataSet() {
    }

    public String getCommandLine() {
        return commandLine;
    }

    public CommandDataSet setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public String getImage() {
        return image;
    }

    public CommandDataSet setImage(String image) {
        this.image = image;
        return this;
    }
}
