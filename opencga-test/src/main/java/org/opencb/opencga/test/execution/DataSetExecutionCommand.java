package org.opencb.opencga.test.execution;

public class DataSetExecutionCommand {

    private String commandLine;
    private String image;

    public DataSetExecutionCommand() {
    }

    public String getCommandLine() {
        return commandLine;
    }

    public DataSetExecutionCommand setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public String getImage() {
        return image;
    }

    public DataSetExecutionCommand setImage(String image) {
        this.image = image;
        return this;
    }
}
