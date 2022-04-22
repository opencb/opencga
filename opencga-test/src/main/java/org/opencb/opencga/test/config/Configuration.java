package org.opencb.opencga.test.config;


import java.util.List;

public class Configuration {

    private Execution execution;
    private List<Environment> envs;
    private Logger logger;
    private Mutator mutator;

    public Configuration() {

    }

    @Override
    public String toString() {
        return "Configuration{" +
                "execution=" + execution +
                ", envs=" + envs +
                ", logger=" + logger +
                ", mutator=" + mutator +
                '}';
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

    public Mutator getMutator() {
        return mutator;
    }

    public Configuration setMutator(Mutator mutator) {
        this.mutator = mutator;
        return this;
    }
}
