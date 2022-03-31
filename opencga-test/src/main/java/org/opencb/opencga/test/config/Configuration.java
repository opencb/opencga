package org.opencb.opencga.test.config;

import java.util.List;

public class Configuration {

    private Execution execution;
    private List<Environment> envs;
    private Logger logger;

    public Configuration() {

    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Configuration{\n");
        sb.append("execution=").append(execution).append("\n");
        sb.append("envs=").append(envs).append("\n");
        sb.append("logger=").append(logger).append("\n");
        sb.append("}\n");
        return sb.toString();
    }

    public Execution getExecution() {
        return execution;
    }

    public Configuration setExecution(Execution execution) {
        this.execution = execution;
        return this;
    }

    public List<Environment> getEnvs() {
        return envs;
    }

    public Configuration setEnvs(List<Environment> envs) {
        this.envs = envs;
        return this;
    }

    public Logger getLogger() {
        return logger;
    }

    public Configuration setLogger(Logger logger) {
        this.logger = logger;
        return this;
    }
}
