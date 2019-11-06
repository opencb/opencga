package org.opencb.opencga.core.config;

import java.util.ArrayList;
import java.util.List;


public class Analysis {

    private String scratchDir;

    private IndexConfiguration index;

    private Execution execution;

    private List<FrameworkConfiguration> frameworks;

    public Analysis() {
        index = new IndexConfiguration();
        execution = new Execution();
        frameworks = new ArrayList<>();
    }

    public String getScratchDir() {
        return scratchDir;
    }

    public Analysis setScratchDir(String scratchDir) {
        this.scratchDir = scratchDir;
        return this;
    }

    public IndexConfiguration getIndex() {
        return index;
    }

    public Analysis setIndex(IndexConfiguration index) {
        this.index = index;
        return this;
    }

    public Execution getExecution() {
        return execution;
    }

    public Analysis setExecution(Execution execution) {
        this.execution = execution;
        return this;
    }

    public List<FrameworkConfiguration> getFrameworks() {
        return frameworks;
    }

    public Analysis setFrameworks(List<FrameworkConfiguration> frameworks) {
        this.frameworks = frameworks;
        return this;
    }

    public static class IndexConfiguration {

        public IndexConfiguration() {
            this.variant = new VariantIndexConfiguration();
        }

        private VariantIndexConfiguration variant;

        public static class VariantIndexConfiguration {
            private int maxConcurrentJobs;

            public int getMaxConcurrentJobs() {
                return maxConcurrentJobs;
            }

            public VariantIndexConfiguration setMaxConcurrentJobs(int maxConcurrentJobs) {
                this.maxConcurrentJobs = maxConcurrentJobs;
                return this;
            }
        }

        public VariantIndexConfiguration getVariant() {
            return variant;
        }

        public IndexConfiguration setVariant(VariantIndexConfiguration variant) {
            this.variant = variant;
            return this;
        }
    }

}
