package org.opencb.opencga.test.execution.models;

import java.util.List;

public class DatasetExecutionFile {

    private List<String> outputFilenames;
    private String inputFilename;
    private List<DataSetExecutionCommand> commands;

    public DatasetExecutionFile() {
    }

    public List<String> getOutputFilenames() {
        return outputFilenames;
    }

    public DatasetExecutionFile setOutputFilenames(List<String> outputFilenames) {
        this.outputFilenames = outputFilenames;
        return this;
    }

    public String getInputFilename() {
        return inputFilename;
    }

    public DatasetExecutionFile setInputFilename(String inputFilename) {
        this.inputFilename = inputFilename;
        return this;
    }

    public List<DataSetExecutionCommand> getCommands() {
        return commands;
    }

    public DatasetExecutionFile setCommands(List<DataSetExecutionCommand> commands) {
        this.commands = commands;
        return this;
    }


}
