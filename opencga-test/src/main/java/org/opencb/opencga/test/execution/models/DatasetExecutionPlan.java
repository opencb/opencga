package org.opencb.opencga.test.execution.models;

import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.test.config.Environment;

import java.util.List;

public class DatasetExecutionPlan {


    /* Environment configuration */
    private Environment environment;

    /* Map with input fastq file name as key and list of commands for this filename as value */
    private List<DatasetExecutionFile> datasetExecutionFiles;


    public DatasetExecutionPlan(Environment environment) {
        this.environment = environment;

    }

    public void simulate() {
        PrintUtils.println(environment.getId(), PrintUtils.Color.YELLOW);
        for (DatasetExecutionFile executionFile : datasetExecutionFiles) {
            PrintUtils.println("    " + executionFile.getInputFilename(), PrintUtils.Color.GREEN);
            for (DataSetExecutionCommand command : executionFile.getCommands()) {
                PrintUtils.println("    " + command.getCommandLine(), PrintUtils.Color.WHITE);
            }
        }
    }

    public Environment getEnvironment() {
        return environment;
    }

    public DatasetExecutionPlan setEnvironment(Environment environment) {
        this.environment = environment;
        return this;
    }

    public List<DatasetExecutionFile> getDatasetExecutionFiles() {
        return datasetExecutionFiles;
    }

    public DatasetExecutionPlan setDatasetExecutionFiles(List<DatasetExecutionFile> datasetExecutionFiles) {
        this.datasetExecutionFiles = datasetExecutionFiles;
        return this;
    }
}
