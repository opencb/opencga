package org.opencb.opencga.test.plan;

import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.test.config.Environment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatasetPlanExecution {


    /* Environment configuration */
    private Environment environment;

    /* Map with input fastq file name as key and list of commands for this filename as value */
    private Map<String, List<String>> commands;

    public DatasetPlanExecution(Environment environment) {
        this.environment = environment;
        commands = new HashMap<>();
    }

    public void simulate() {
        PrintUtils.println(environment.getId(), PrintUtils.Color.YELLOW);
        for (String filename : commands.keySet()) {
            PrintUtils.println("    " + filename, PrintUtils.Color.GREEN);
            for (String command : commands.get(filename)) {
                PrintUtils.println("    " + command, PrintUtils.Color.WHITE);
            }
        }
    }

    public Environment getEnvironment() {
        return environment;
    }

    public DatasetPlanExecution setEnvironment(Environment environment) {
        this.environment = environment;
        return this;
    }

    public Map<String, List<String>> getCommands() {
        return commands;
    }

    public DatasetPlanExecution setCommands(Map<String, List<String>> commands) {
        this.commands = commands;
        return this;
    }
}
