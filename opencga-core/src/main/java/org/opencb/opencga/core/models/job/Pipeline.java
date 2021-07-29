package org.opencb.opencga.core.models.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opencb.commons.datastore.core.ObjectMap;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class Pipeline {

    private String id;
    private String description;

    private boolean disabled;

    private Options options;
    private List<Job> jobs;

    private static final String DEFAULT_PIPELINE_FORMAT = "yaml";

    public Pipeline() {
    }

    public Pipeline(String id, String description, boolean disabled, Options options, List<Job> jobs) {
        this.id = id;
        this.description = description;
        this.disabled = disabled;
        this.options = options;
        this.jobs = jobs;
    }

    public static Pipeline load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, DEFAULT_PIPELINE_FORMAT);
    }

    public static Pipeline load(InputStream configurationInputStream, String format) throws IOException {
        if (configurationInputStream == null) {
            throw new IOException("Pipeline file not found");
        }
        Pipeline pipeline;
        ObjectMapper objectMapper;
        try {
            switch (format) {
                case "json":
                    objectMapper = new ObjectMapper();
                    pipeline = objectMapper.readValue(configurationInputStream, Pipeline.class);
                    break;
                case "yml":
                case "yaml":
                default:
                    objectMapper = new ObjectMapper(new YAMLFactory());
                    pipeline = objectMapper.readValue(configurationInputStream, Pipeline.class);
                    break;
            }
        } catch (IOException e) {
            throw new IOException("Pipeline file could not be parsed: " + e.getMessage(), e);
        }

        return pipeline;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Pipeline{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", disabled=").append(disabled);
        sb.append(", options=").append(options);
        sb.append(", jobs=").append(jobs);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Pipeline setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Pipeline setDescription(String description) {
        this.description = description;
        return this;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public Pipeline setDisabled(boolean disabled) {
        this.disabled = disabled;
        return this;
    }

    public Options getOptions() {
        return options;
    }

    public Pipeline setOptions(Options options) {
        this.options = options;
        return this;
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public Pipeline setJobs(List<Job> jobs) {
        this.jobs = jobs;
        return this;
    }

    public static class Options {

        private int retry;
        private int timeout;

        public Options() {
        }

        public Options(int retry, int timeout) {
            this.retry = retry;
            this.timeout = timeout;
        }

        public static Options init() {
            return new Options(3, 3600);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Options{");
            sb.append("retry=").append(retry);
            sb.append(", timeout=").append(timeout);
            sb.append('}');
            return sb.toString();
        }

        public int getRetry() {
            return retry;
        }

        public Options setRetry(int retry) {
            this.retry = retry;
            return this;
        }

        public int getTimeout() {
            return timeout;
        }

        public Options setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }
    }

    public static class Job {

        private String id;
        private String toolId;
        private ObjectMap params;
        private List<String> input;
        private List<String> output;
        private List<String> dependsOn;

        public Job() {
        }

        public Job(String id, String toolId, ObjectMap params, List<String> input, List<String> output, List<String> dependsOn) {
            this.id = id;
            this.toolId = toolId;
            this.params = params;
            this.input = input;
            this.output = output;
            this.dependsOn = dependsOn;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Job{");
            sb.append("id='").append(id).append('\'');
            sb.append(", toolId='").append(toolId).append('\'');
            sb.append(", params=").append(params);
            sb.append(", input=").append(input);
            sb.append(", output=").append(output);
            sb.append(", dependsOn=").append(dependsOn);
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public Job setId(String id) {
            this.id = id;
            return this;
        }

        public String getToolId() {
            return toolId;
        }

        public Job setToolId(String toolId) {
            this.toolId = toolId;
            return this;
        }

        public ObjectMap getParams() {
            return params;
        }

        public Job setParams(ObjectMap params) {
            this.params = params;
            return this;
        }

        public List<String> getInput() {
            return input;
        }

        public Job setInput(List<String> input) {
            this.input = input;
            return this;
        }

        public List<String> getOutput() {
            return output;
        }

        public Job setOutput(List<String> output) {
            this.output = output;
            return this;
        }

        public List<String> getDependsOn() {
            return dependsOn;
        }

        public Job setDependsOn(List<String> dependsOn) {
            this.dependsOn = dependsOn;
            return this;
        }
    }

}
