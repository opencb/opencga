package org.opencb.opencga.test.execution;

import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.test.config.Environment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatasetExecutionPlan {


    /* Environment configuration */
    private Environment environment;

    /* Map with input fastq file name as key and list of commands for this filename as value */
    private Map<String, List<DataSetExecutionCommand>> commands;

    public DatasetExecutionPlan(Environment environment) {
        this.environment = environment;
        commands = new HashMap<>();
    }

    public void simulate() {
        PrintUtils.println(environment.getId(), PrintUtils.Color.YELLOW);
        for (String filename : commands.keySet()) {
            PrintUtils.println("    " + filename, PrintUtils.Color.GREEN);
            for (DataSetExecutionCommand command : commands.get(filename)) {
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

    public Map<String, List<DataSetExecutionCommand>> getCommands() {
        return commands;
    }

    public DatasetExecutionPlan setCommands(Map<String, List<DataSetExecutionCommand>> commands) {
        this.commands = commands;
        return this;
    }
}
